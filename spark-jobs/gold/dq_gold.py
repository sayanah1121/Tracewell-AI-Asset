"""Data-quality checks for the gold marts and the stream they are built from.

Results go to ops.dq_results (suite 'gold'); a failed fatal check fails the task. SLO checks
(latency, lateness, DLQ, unknown assets) only warn: they describe the stream, not a broken mart.
"""
import argparse
import sys
from datetime import datetime, timedelta, timezone
from functools import reduce

from pyspark.sql import functions as F

from common import log, spark_session

PII = [  # same detectors as sql/checks/stream_checks.sql
    r"AKIA[0-9A-Z]{16}|sk-[A-Za-z0-9]{20,}|gh[pousr]_[A-Za-z0-9]{30,}",
    r"\b\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}\b|\b3[47]\d{13}\b",
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    r"\b[A-Z]{5}\d{4}[A-Z]\b|\b[2-9]\d{3} \d{4} \d{4}\b|\+91 \d{5} \d{5}",
]
COMMIT_LAG_S = 120  # Flink commits to Iceberg on each checkpoint (20 s); allow for a slow one


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2)
    args = ap.parse_args()
    spark = spark_session("dq-gold")
    start = datetime.now(timezone.utc).date() - timedelta(days=args.days - 1)
    in_window = F.col("day") >= F.lit(start)
    events = spark.table("tracewell.silver.fct_ai_events")
    results = []  # (check, passed, detail, fatal)

    # Posture must account for every event Flink had committed when the mart was built, and no more.
    posture = spark.table("tracewell.gold.fleet_risk_posture").where(in_window).groupBy("day").agg(
        F.sum("events").alias("mart"), F.min("built_at").alias("built_at"))
    silver = events.withColumn("day", F.to_date("ts_event")).where(in_window)
    bounds = posture.join(silver, "day").groupBy("day", "mart").agg(
        F.sum(F.when(F.col("processed_at") <= F.col("built_at") - F.expr(f"INTERVAL {COMMIT_LAG_S} SECONDS"), 1)
              .otherwise(0)).alias("low"),
        F.sum(F.when(F.col("processed_at") <= F.col("built_at"), 1).otherwise(0)).alias("high")).collect()
    off = [f"{r.day}: mart {r.mart} not in [{r.low}, {r.high}]" for r in bounds if not r.low <= r.mart <= r.high]
    results.append(("posture_reconciles_with_silver", bool(bounds) and not off,
                    "; ".join(off) or f"{len(bounds)} days reconcile" if bounds else "no posture rows in window", True))

    for table, keys in [("shadow_ai", ["day", "asset_id"]), ("fleet_risk_posture", ["day", "department"]),
                        ("policy_hits", ["day", "policy_id", "department", "asset_id"]),
                        ("ai_roi", ["month", "tool_id", "department"]), ("pipeline_health", ["hour"]),
                        ("compliance_evidence", ["asset_id"]), ("ocsf_findings", ["event_id"])]:
        dup = spark.table(f"tracewell.gold.{table}").groupBy(*keys).count().where("count > 1").count()
        results.append((f"{table}_unique_{'_'.join(keys)}", dup == 0, f"{dup} duplicate keys", True))

    # Every finding must be a well-formed OCSF Detection Finding a SIEM can route without a custom parser.
    ocsf = spark.table("tracewell.gold.ocsf_findings").where(in_window)
    j = lambda path: F.get_json_object("ocsf", path)  # noqa: E731
    bad_ocsf = ocsf.where((j("$.class_uid") != "2004") | ~j("$.severity_id").isin("2", "3", "4", "5")
                          | j("$.time").isNull() | j("$.finding_info.uid").isNull() | j("$.metadata.version").isNull()).count()
    results.append(("ocsf_findings_well_formed", bad_ocsf == 0, f"{bad_ocsf} malformed of {ocsf.count()} findings", True))

    roi = spark.table("tracewell.gold.ai_roi")
    bad_seats = roi.where("idle_seats + active_users - users_without_seat != seats OR idle_seats < 0").count()
    results.append(("ai_roi_seat_arithmetic", bad_seats == 0, f"{bad_seats} rows where seats do not add up", True))

    comp = spark.table("tracewell.gold.compliance_evidence")
    n_comp, n_dim = comp.count(), spark.table("tracewell.silver.dim_asset_risk").where("is_current").count()
    results.append(("compliance_covers_catalog", n_comp == n_dim, f"{n_comp} evidence rows vs {n_dim} current assets", True))
    no_evidence = comp.where("in_use_30d AND band IN ('High', 'Critical') AND size(evidence) = 0").count()
    results.append(("risky_assets_in_use_have_evidence", no_evidence == 0,
                    f"{no_evidence} High/Critical assets in use without a mapped control", True))

    shadow_events = silver.where("NOT asset_known OR NOT sanctioned_at_event").select("day", "asset_id").distinct()
    shadow_sanctioned = (spark.table("tracewell.gold.shadow_ai").where(in_window)
                         .join(shadow_events, ["day", "asset_id"], "left_anti").count())
    results.append(("shadow_ai_only_unsanctioned", shadow_sanctioned == 0,
                    f"{shadow_sanctioned} shadow rows without an unsanctioned or unknown event", True))

    raw = silver.where(reduce(lambda a, b: a | b, [F.col("prompt_text").rlike(p) for p in PII])).count()
    results.append(("silver_no_raw_pii", raw == 0, f"{raw} events with raw sensitive data in the window", True))

    # SLOs (warn only), over the hours in the window.
    h = spark.table("tracewell.gold.pipeline_health").where(in_window).agg(
        F.max("latency_p95_ms").alias("p95"), F.sum("events_out").alias("ev"), F.sum("late_events").alias("late"),
        F.sum("unknown_asset_events").alias("unk"), F.sum("dlq_records").alias("dlq"),
        F.sum("records_in").alias("rin")).first()
    ev, rin = h.ev or 0, h.rin or 0
    pct = lambda part, whole: 100.0 * (part or 0) / whole if whole else 0.0  # noqa: E731
    results.append(("slo_p95_latency_under_1s", (h.p95 or 0) < 1000, f"worst hourly p95 {h.p95} ms", False))
    results.append(("slo_late_under_5pct", pct(h.late, ev) < 5, f"{pct(h.late, ev):.2f}% late", False))
    results.append(("slo_unknown_assets_under_2pct", pct(h.unk, ev) < 2, f"{pct(h.unk, ev):.2f}% unknown", False))
    results.append(("slo_dlq_under_0_1pct", pct(h.dlq, rin + (h.dlq or 0)) < 0.1,
                    f"{pct(h.dlq, rin + (h.dlq or 0)):.3f}% to DLQ", False))
    last = events.agg(F.max("processed_at")).first()[0]
    age_min = (datetime.now(timezone.utc).replace(tzinfo=None) - last).total_seconds() / 60 if last else None
    results.append(("stream_flowing_15min", age_min is not None and age_min <= 15,
                    f"last event processed {age_min:.0f} min ago" if age_min is not None else "no events", False))

    spark.createDataFrame([(r[0], r[1], r[2]) for r in results], "check_name string, passed boolean, detail string") \
        .select(F.current_timestamp().alias("run_at"), F.lit("gold").alias("suite"), "*") \
        .writeTo("tracewell.ops.dq_results").append()
    for check, passed, detail, fatal in results:
        log(f"{'PASS' if passed else ('FAIL' if fatal else 'WARN')}  {check}: {detail}")
    failed = [r for r in results if not r[1] and r[3]]
    spark.stop()
    if failed:
        sys.exit(f"{len(failed)} fatal data-quality checks failed")


if __name__ == "__main__":
    main()
