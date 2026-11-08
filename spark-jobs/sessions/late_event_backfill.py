"""Repair activity sessions after late events (Flink side output `late-events`).

Flink closes a session window once the watermark passes the last event + gap; an event that arrives after
that (an offline laptop flushing hours of activity) can no longer change the window, so Flink sends it to
`late-events`. Every event still reaches silver.fct_ai_events, so this job re-sessionizes the affected
devices from silver and swaps the result into silver.fct_sessions in one atomic MERGE:

  * sessions that are new or changed are inserted / updated (source = 'backfill');
  * stream sessions they supersede (overlapping, different id) are deleted;
  * sessions still open (last event within gap + settle minutes) are skipped and picked up next run.

Idempotent: a rerun over the same late events finds nothing to change. `--full` re-sessionizes all of
silver (initial load, or after changing the gap).
"""
import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import log, spark_session
from sessionize import sessionize

SESSIONS = "tracewell.silver.fct_sessions"
EVENTS = "tracewell.silver.fct_ai_events"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48, help="late events received in this many hours")
    ap.add_argument("--gap-minutes", type=int, default=10, help="must match the Flink job's --session-gap-minutes")
    ap.add_argument("--settle-minutes", type=int, default=15, help="extra wait before a session counts as closed")
    ap.add_argument("--full", action="store_true", help="re-sessionize every device from all of silver")
    ap.add_argument("--bootstrap", default="redpanda:9092")
    args = ap.parse_args()

    spark = spark_session("late-event-backfill")
    # Flink keeps appending to this table; snapshot isolation lets the MERGE commit next to those appends.
    spark.sql(f"ALTER TABLE {SESSIONS} SET TBLPROPERTIES ('write.merge.isolation-level'='snapshot')")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(minutes=args.gap_minutes + args.settle_minutes)
    events = spark.table(EVENTS)

    if args.full:
        affected = events.groupBy("device_id", "identity").agg(F.min("ts_event").alias("lo"))
        n_late = None
    else:
        late = (spark.read.format("kafka").option("kafka.bootstrap.servers", args.bootstrap)
                .option("subscribe", "late-events").option("startingOffsets", "earliest").load()
                .where(F.col("timestamp") >= F.lit(now - timedelta(hours=args.hours)))
                .select(F.from_json(F.col("value").cast("string"),
                                    "event_id string, ts_event long, device_id string, identity string").alias("e"))
                .select("e.*").localCheckpoint())
        n_late = late.count()
        affected = late.groupBy("device_id", "identity").agg(
            F.timestamp_millis(F.min("ts_event")).alias("lo"))
    affected = affected.localCheckpoint()
    n_devices = affected.count()
    if n_devices == 0:
        log(f"no late events in the last {args.hours} h; nothing to repair")
        spark.stop()
        return

    scoped = (events.join(F.broadcast(affected), ["device_id", "identity"])
              .where(F.col("ts_event") >= F.col("lo") - F.expr("INTERVAL 1 DAY")))
    fresh = (sessionize(scoped.drop("lo"), args.gap_minutes * 60_000)
             .join(F.broadcast(affected), ["device_id", "identity"])
             .where((F.col("session_end") >= F.col("lo")) & (F.col("session_end") < F.lit(cutoff)))
             .drop("lo").withColumn("built_at", F.current_timestamp()).localCheckpoint())

    # Stream sessions covering the same stretch of time, under another id, are superseded.
    span = fresh.groupBy("device_id", "identity").agg(F.min("session_start").alias("s_lo"),
                                                      F.max("session_end").alias("s_hi"))
    existing = spark.table(SESSIONS)
    stale = (existing.join(F.broadcast(span), ["device_id", "identity"])
             .where((F.col("session_end") >= F.col("s_lo")) & (F.col("session_start") <= F.col("s_hi")))
             .join(fresh.select("session_id"), "session_id", "left_anti")
             .select("session_id").distinct().localCheckpoint())
    changed = (fresh.alias("f").join(existing.alias("x"), "session_id", "left")
               .where("x.session_id IS NULL OR x.event_count <> f.event_count OR x.session_end <> f.session_end")
               .select("f.*").localCheckpoint())
    n_new = changed.join(existing.select("session_id"), "session_id", "left_anti").count()
    n_changed, n_stale = changed.count(), stale.count()

    if n_changed + n_stale:
        changed.withColumn("_op", F.lit("upsert")).unionByName(
            stale.withColumn("_op", F.lit("delete")), allowMissingColumns=True).createOrReplaceTempView("_repair")
        cols = [c for c in changed.columns]
        spark.sql(f"""
            MERGE INTO {SESSIONS} t USING _repair s ON t.session_id = s.session_id
            WHEN MATCHED AND s._op = 'delete' THEN DELETE
            WHEN MATCHED AND s._op = 'upsert' THEN UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in cols)}
            WHEN NOT MATCHED AND s._op = 'upsert' THEN INSERT ({", ".join(cols)}) VALUES ({", ".join(f"s.{c}" for c in cols)})
        """)
    log(f"{'full rebuild' if args.full else f'{n_late} late events'} on {n_devices} device(s): "
        f"{n_new} sessions inserted, {n_changed - n_new} updated, {n_stale} superseded stream sessions deleted "
        f"(sessions ending after {cutoff:%H:%M} UTC wait for the next run)")
    spark.stop()


if __name__ == "__main__":
    main()
