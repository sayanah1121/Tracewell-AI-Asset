"""Investigate one activity session: what happened, how risky each tool was *then* vs *today*, and who else is exposed.

  spark-submit investigate.py --session <session_id>
  spark-submit investigate.py --event <event_id>                 # the session that contains this event

For every event in the session it puts three views of the asset's risk side by side:

  recorded   what Flink actually enforced (the catalog in its broadcast state at verdict time)
  as-of      SCD2 as-of join on silver.dim_asset_risk: the version whose [valid_from, valid_to) holds ts_event
  time travel the dim_asset_risk table exactly as it was committed at ts_event (Iceberg snapshot)

plus today's profile. As-of and time travel must agree, except for events that fall between a version's
valid_from (the scoring run's clock) and the moment that version was committed; those are flagged with the
lag. Blast radius: every other device and person that used the session's risky assets in the last 30 days.

Writes gold.investigations, gold.incident_timeline and gold.incident_blast_radius (append, keyed by
investigation_id) and prints a report.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import log, spark_session

EVENTS = "tracewell.silver.fct_ai_events"
SESSIONS = "tracewell.silver.fct_sessions"
DIM = "tracewell.silver.dim_asset_risk"
RISKY = ["High", "Critical"]


# ───────────────────────────── pure parts (unit-tested) ─────────────────────────────

def as_of(events: DataFrame, dim: DataFrame) -> DataFrame:
    """SCD2 as-of join: the version of each asset valid at the event's time."""
    v = dim.select("asset_id", F.col("valid_from").alias("asof_valid_from"), F.col("valid_to").alias("asof_valid_to"),
                   F.col("risk_score").cast("double").alias("score_asof"), F.col("band").alias("band_asof"),
                   F.col("sanctioned").alias("sanctioned_asof"))
    cond = ((events.asset_id == v.asset_id) & (events.ts_event >= v.asof_valid_from) & (events.ts_event < v.asof_valid_to))
    return events.join(v, cond, "left").drop(v.asset_id)


def snapshot_at(events: DataFrame, snapshots: DataFrame) -> DataFrame:
    """The dim_asset_risk snapshot that was current at each event: the last one committed at or before ts_event."""
    s = snapshots.select(F.col("snapshot_id").alias("tt_snapshot_id"), F.col("committed_at").alias("tt_committed_at"))
    cand = events.select("event_id", "ts_event").join(s, F.col("tt_committed_at") <= F.col("ts_event"))
    best = cand.groupBy("event_id").agg(F.max_by("tt_snapshot_id", "tt_committed_at").alias("tt_snapshot_id"),
                                        F.max("tt_committed_at").alias("tt_committed_at"))
    return events.join(best, "event_id", "left")


def compare(timeline: DataFrame, version_commits: DataFrame) -> DataFrame:
    """Flag where time travel and the as-of join disagree, and whether the commit lag explains it.

    version_commits: (asset_id, asof_valid_from, version_committed_at), the commit time of each SCD2 version.
    """
    t = timeline.join(version_commits, ["asset_id", "asof_valid_from"], "left")
    agrees = ((F.col("band_tt").eqNullSafe(F.col("band_asof"))) & (F.col("score_tt").eqNullSafe(F.col("score_asof")))
              & (F.col("sanctioned_tt").eqNullSafe(F.col("sanctioned_asof"))))
    in_lag = (F.col("ts_event") >= F.col("asof_valid_from")) & (F.col("ts_event") < F.col("version_committed_at"))
    return (t.withColumn("tt_agrees", agrees)
            .withColumn("mismatch_reason", F.when(agrees, None)
                        .when(in_lag, F.format_string("event between valid_from and commit of this version (%d s lag)",
                                                      (F.unix_timestamp("version_committed_at") - F.unix_timestamp("asof_valid_from")).cast("int")))
                        .otherwise("unexplained")))


def blast_radius(usage: DataFrame, assets: DataFrame, current: DataFrame) -> DataFrame:
    """Who else is exposed: for each risky asset of the session, every device/person that used it in the window."""
    u = usage.join(assets, "asset_id")
    return (u.groupBy("asset_id").agg(
                F.countDistinct("device_id").alias("devices"), F.countDistinct("identity").alias("people"),
                F.sort_array(F.collect_set("department")).alias("departments"), F.count("*").alias("events"),
                F.sum(F.when(F.col("sensitive_count") > 0, 1).otherwise(0)).cast("long").alias("sensitive_events"),
                F.sum(F.when(F.col("action") == "block", 1).otherwise(0)).cast("long").alias("blocks"),
                F.min("ts_event").alias("first_seen"), F.max("ts_event").alias("last_seen"))
            .join(current.select("asset_id", "asset_name", F.col("band").alias("band_today"),
                                 F.col("risk_score").cast("double").alias("score_today")), "asset_id", "left")
            .withColumn("band_today", F.coalesce("band_today", F.lit("Unknown")))   # not in the catalog
            .join(assets, "asset_id"))


# ───────────────────────────── IO ─────────────────────────────

def find_session(spark: SparkSession, session_id: str | None, event_id: str | None):
    sessions = spark.table(SESSIONS)
    if session_id:
        rows = sessions.where(F.col("session_id") == session_id).collect()
    else:
        e = spark.table(EVENTS).where(F.col("event_id") == event_id).select("device_id", "identity", "ts_event").collect()
        if not e:
            sys.exit(f"event {event_id} not found")
        rows = sessions.where((F.col("device_id") == e[0].device_id) & (F.col("identity") == e[0].identity)
                              & (F.col("session_start") <= e[0].ts_event) & (F.col("session_end") >= e[0].ts_event)).collect()
    if not rows:
        sys.exit("session not found (still open? sessions are written when they end; late ones by late_event_backfill)")
    return rows[0]


def time_travel(spark: SparkSession, timeline: DataFrame) -> DataFrame:
    """Read dim_asset_risk at each snapshot the timeline needs (one read per distinct snapshot)."""
    assets = [r.asset_id for r in timeline.select("asset_id").distinct().collect()]
    parts = []
    for r in timeline.select("tt_snapshot_id").distinct().where("tt_snapshot_id IS NOT NULL").collect():
        parts.append(spark.read.option("snapshot-id", r.tt_snapshot_id).table(DIM)
                     .where(F.col("is_current") & F.col("asset_id").isin(assets))
                     .select("asset_id", F.lit(r.tt_snapshot_id).alias("tt_snapshot_id"),
                             F.col("risk_score").cast("double").alias("score_tt"), F.col("band").alias("band_tt"),
                             F.col("sanctioned").alias("sanctioned_tt")))
    if not parts:
        return timeline.withColumn("score_tt", F.lit(None).cast("double")).withColumn("band_tt", F.lit(None).cast("string")) \
            .withColumn("sanctioned_tt", F.lit(None).cast("boolean"))
    tt = parts[0]
    for p in parts[1:]:
        tt = tt.unionByName(p)
    return timeline.join(tt, ["asset_id", "tt_snapshot_id"], "left")


def version_commits(spark: SparkSession, assets: list[str]) -> DataFrame:
    """Commit time of each SCD2 version: the first snapshot in which that (asset_id, valid_from) row exists."""
    snaps = spark.table(f"{DIM}.snapshots").select("snapshot_id", "committed_at").orderBy("committed_at").collect()
    seen = None
    for s in snaps:
        rows = (spark.read.option("snapshot-id", s.snapshot_id).table(DIM).where(F.col("asset_id").isin(assets))
                .select("asset_id", F.col("valid_from").alias("asof_valid_from"))
                .withColumn("version_committed_at", F.lit(s.committed_at)))
        seen = rows if seen is None else seen.unionByName(rows)
    return seen.groupBy("asset_id", "asof_valid_from").agg(F.min("version_committed_at").alias("version_committed_at"))


def append(df: DataFrame, table: str) -> None:
    if df.sparkSession.catalog.tableExists(table):
        df.writeTo(table).append()
    else:
        df.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--session")
    g.add_argument("--event")
    ap.add_argument("--blast-days", type=int, default=30)
    args = ap.parse_args()
    spark = spark_session("investigate")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS tracewell.gold")

    s = find_session(spark, args.session, args.event)
    inv_id = f"INV-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    events = (spark.table(EVENTS)
              .where((F.col("device_id") == s.device_id) & (F.col("identity") == s.identity)
                     & F.col("ts_event").between(s.session_start, s.session_end)))
    dim = spark.table(DIM)
    current = dim.where("is_current")

    t = as_of(events, dim)
    t = snapshot_at(t, spark.table(f"{DIM}.snapshots"))
    t = time_travel(spark, t)
    commits = version_commits(spark, [r.asset_id for r in events.select("asset_id").distinct().collect()]).localCheckpoint()
    t = compare(t, commits)
    t = (t.join(current.select("asset_id", "asset_name", F.col("risk_score").cast("double").alias("score_today"),
                               F.col("band").alias("band_today"), F.col("sanctioned").alias("sanctioned_today")),
                "asset_id", "left")
         .withColumn("changed_since_event", ~(F.col("band_today").eqNullSafe(F.col("band_asof"))
                                              & F.col("sanctioned_today").eqNullSafe(F.col("sanctioned_asof"))))
         .withColumn("seq", F.row_number().over(Window.orderBy("ts_event", "event_id")))
         .select(F.lit(inv_id).alias("investigation_id"), "seq", "ts_event", "event_id", "sensor", "event_type",
                 "asset_id", F.coalesce("asset_name", "asset_id").alias("asset_name"), "asset_known", "action",
                 "policy_id", "sensitive_types", "file_name", "dest_host", "bytes_out",
                 F.col("risk_score_at_event").alias("score_recorded"), F.col("band_at_event").alias("band_recorded"),
                 F.col("sanctioned_at_event").alias("sanctioned_recorded"),
                 "score_asof", "band_asof", "sanctioned_asof", "asof_valid_from",
                 "score_tt", "band_tt", "sanctioned_tt", "tt_snapshot_id", "tt_committed_at", "tt_agrees", "mismatch_reason",
                 "score_today", "band_today", "sanctioned_today", "changed_since_event")
         .localCheckpoint())

    risky = (t.where(F.col("band_asof").isin(RISKY) | F.col("band_today").isin(RISKY) | ~F.col("asset_known")
                     | F.col("action").isin("block", "warn"))
             .select("asset_id").distinct()
             .withColumn("reason", F.lit("risky, unknown or policy hit in this session")))
    usage = spark.table(EVENTS).where(F.col("ts_event") >= F.lit(s.session_end) - F.expr(f"INTERVAL {args.blast_days} DAYS"))
    blast = (blast_radius(usage, risky, current)
             .select(F.lit(inv_id).alias("investigation_id"), "asset_id", F.coalesce("asset_name", "asset_id").alias("asset_name"),
                     "band_today", "score_today", "devices", "people", "departments", "events", "sensitive_events",
                     "blocks", "first_seen", "last_seen")
             .localCheckpoint())

    st = t.agg(F.count("*").alias("n"), F.sum(F.col("tt_agrees").cast("int")).alias("agree"),
               F.sum(F.when(F.col("mismatch_reason") == "unexplained", 1).otherwise(0)).alias("unexplained"),
               F.sum(F.col("changed_since_event").cast("int")).alias("changed"),
               F.sum(F.when(F.col("score_recorded") != F.col("score_asof"), 1).otherwise(0)).alias("recorded_diff"),
               F.countDistinct(F.when(F.col("changed_since_event"), F.col("asset_id"))).alias("changed_assets")).first()
    header = spark.createDataFrame([(
        inv_id, s.session_id, s.device_id, s.identity, s.department, s.session_start, s.session_end, st.n,
        s.max_band, int(st.changed_assets), int(st.changed), round(100.0 * st.agree / st.n, 2) if st.n else None,
        int(st.unexplained), int(st.recorded_diff), blast.count(),
        blast.agg(F.max("devices")).first()[0])],
        "investigation_id string, session_id string, device_id string, identity string, department string, "
        "session_start timestamp, session_end timestamp, events long, max_band_in_session string, "
        "assets_changed_since int, events_changed_since int, tt_agreement_pct double, tt_unexplained int, "
        "recorded_vs_asof_diffs int, risky_assets int, max_blast_devices long"
    ).withColumn("created_at", F.current_timestamp())

    append(header, "tracewell.gold.investigations")
    append(t, "tracewell.gold.incident_timeline")
    append(blast, "tracewell.gold.incident_blast_radius")

    h = header.first()
    log(f"{inv_id}: session {s.session_id} · {s.identity} on {s.device_id} · {s.session_start:%Y-%m-%d %H:%M}–{s.session_end:%H:%M} UTC · {h.events} events")
    log(f"  risk changed since the event for {h.assets_changed_since} asset(s) ({h.events_changed_since} events); "
        f"recorded verdict score differs from SCD2 as-of on {h.recorded_vs_asof_diffs} events")
    log(f"  time travel vs as-of: {h.tt_agreement_pct}% agree, {h.tt_unexplained} unexplained mismatches")
    for r in t.where("changed_since_event OR NOT tt_agrees").groupBy(
            "asset_name", "band_recorded", "band_asof", "band_tt", "band_today", "mismatch_reason").count().collect():
        log(f"  {r.asset_name}: recorded {r.band_recorded} · as-of {r.band_asof} · time travel {r.band_tt} · today "
            f"{r.band_today} ({r['count']} events){' · ' + r.mismatch_reason if r.mismatch_reason else ''}")
    for r in blast.orderBy(F.desc("devices")).limit(10).collect():
        log(f"  blast radius {r.asset_name} ({r.band_today}): {r.devices} devices, {r.people} people, "
            f"{r.sensitive_events} sensitive events, {r.blocks} blocks")
    spark.stop()


if __name__ == "__main__":
    main()
