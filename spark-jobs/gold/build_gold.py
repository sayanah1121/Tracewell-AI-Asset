"""Build one gold mart (or the reference tables) from silver.

Idempotent: a run rebuilds whole days (or whole months for ai_roi) and overwrites only those partitions,
so the hourly DAG can rerun any hour and late events are picked up by the next run.

  spark-submit build_gold.py --mart reference                 # seeds -> silver.dim_licence / dim_identity / dim_device
  spark-submit build_gold.py --mart shadow_ai [--days 2]      # rebuild today and yesterday (UTC)
  spark-submit build_gold.py --mart pipeline_health --full    # rebuild everything
"""
import argparse
import csv
import os
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import marts
from common import log, spark_session

SEED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seed")
POLICY_DIR = os.environ.get("TRACEWELL_POLICY_DIR", "/opt/tracewell/policies")
EVENTS = "tracewell.silver.fct_ai_events"
DIM = "tracewell.silver.dim_asset_risk"
PARTITION = {"shadow_ai": "day", "fleet_risk_posture": "day", "policy_hits": "day", "ai_roi": "month",
             "compliance_evidence": None, "pipeline_health": "day", "ocsf_findings": "day"}
REFERENCE = {  # seed file -> table, schema
    "licences.csv": ("tracewell.silver.dim_licence", "tool_id string, tool_name string, function string, department string, "
                                                "seats int, cost_per_seat_month double"),
    "people.csv": ("tracewell.silver.dim_identity", "identity string, department string"),
    "devices.csv": ("tracewell.silver.dim_device", "device_id string, identity string, department string"),
}


def read_seed(spark: SparkSession, name: str, schema: str) -> DataFrame:
    # Read on the driver: the seed files ship with the job, executors may not see this path.
    with open(os.path.join(SEED_DIR, name), encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    cols = [c.split() for c in schema.split(", ")]
    return spark.createDataFrame(rows).selectExpr(*[f"CAST(`{name}` AS {typ}) AS `{name}`" for name, typ in cols])


def write(df: DataFrame, table: str, partition: str | None) -> int:
    df = df.withColumn("built_at", F.current_timestamp()).localCheckpoint()
    spark = df.sparkSession
    if not spark.catalog.tableExists(table):
        w = df.writeTo(table).using("iceberg").tableProperty("format-version", "2") \
            .tableProperty("write.parquet.compression-codec", "zstd")
        (w.partitionedBy(F.col(partition)) if partition else w).create()
    else:
        # Schema evolution: a column added to a mart is added to the table (Iceberg: metadata-only, no rewrite).
        existing = {f.name for f in spark.table(table).schema}
        for f in df.schema:
            if f.name not in existing:
                spark.sql(f"ALTER TABLE {table} ADD COLUMN `{f.name}` {f.dataType.simpleString()}")
                log(f"{table}: added column {f.name} {f.dataType.simpleString()}")
        df.writeTo(table).overwritePartitions()  # unpartitioned table: replaces the whole table
    return df.count()


def read_policies(spark: SparkSession) -> DataFrame:
    import glob
    import yaml  # ships with Airflow, where the driver runs
    rules = [r for path in sorted(glob.glob(os.path.join(POLICY_DIR, "*.yaml")))
             for r in (yaml.safe_load(open(path, encoding="utf-8")) or [])]
    return spark.createDataFrame([(r["policy_id"], r["name"]) for r in rules], "policy_id string, policy_name string")


def kafka_dlq(spark: SparkSession, bootstrap: str, start: date | None) -> DataFrame:
    try:
        df = (spark.read.format("kafka").option("kafka.bootstrap.servers", bootstrap)
              .option("subscribe", "ai-events-dlq").option("startingOffsets", "earliest").load()
              .select("timestamp"))
        return df.where(F.col("timestamp") >= F.lit(start).cast("timestamp")) if start else df
    except Exception as e:  # noqa: BLE001 - a missing DLQ topic must not stop the health mart
        log(f"DLQ topic not readable ({type(e).__name__}); counting 0 DLQ records")
        return spark.createDataFrame([], "timestamp timestamp")


def since(df: DataFrame, col: str, start: date | None) -> DataFrame:
    return df.where(F.col(col) >= F.lit(start).cast("timestamp")) if start else df


def build(spark: SparkSession, mart: str, start: date | None, bootstrap: str) -> DataFrame:
    events = spark.table(EVENTS)
    current = spark.table(DIM).where("is_current")
    if mart == "shadow_ai":
        return marts.shadow_ai(since(events, "ts_event", start), current,
                               marts.first_seen_per_asset(events.select("asset_id", "ts_event")))
    if mart == "fleet_risk_posture":
        return marts.fleet_risk_posture(since(events, "ts_event", start))
    if mart == "policy_hits":
        return marts.policy_hits(since(events, "ts_event", start), current, read_policies(spark))
    if mart == "ai_roi":
        # Whole months, so a month's active-user count is never computed from a partial month.
        first_month = start.replace(day=1) if start else None
        scoped = since(events, "ts_event", first_month)
        months = scoped.select(F.trunc("ts_event", "month").alias("month")).distinct()
        if first_month:  # also emit months with no events at all (every seat idle)
            today = datetime.now(timezone.utc).date()
            months = months.unionByName(spark.sql(
                f"SELECT explode(sequence(DATE'{first_month}', DATE'{today}', INTERVAL 1 MONTH)) AS month")).distinct()
        return marts.ai_roi(scoped, spark.table("tracewell.silver.dim_licence"), spark.table("tracewell.silver.dim_identity"), months)
    if mart == "compliance_evidence":
        recent = events.where(F.col("ts_event") >= F.expr("current_timestamp() - INTERVAL 30 DAYS"))
        return marts.compliance_evidence(current, recent)
    if mart == "ocsf_findings":
        return marts.ocsf_findings(since(events, "ts_event", start), read_policies(spark))
    if mart == "pipeline_health":
        return marts.pipeline_health(since(spark.table("tracewell.bronze.ai_events"), "ingested_at", start),
                                     since(events, "processed_at", start), kafka_dlq(spark, bootstrap, start),
                                     until=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None))
    raise ValueError(mart)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mart", required=True, choices=["reference", *PARTITION])
    ap.add_argument("--days", type=int, default=2, help="whole UTC days to rebuild, ending today")
    ap.add_argument("--full", action="store_true", help="rebuild every partition")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    args = ap.parse_args()

    spark = spark_session(f"gold-{args.mart}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS tracewell.gold")
    if args.mart == "reference":
        for name, (table, schema) in REFERENCE.items():
            read_seed(spark, name, schema).writeTo(table).using("iceberg").createOrReplace()
            log(f"{table}: {spark.table(table).count()} rows from seed/{name}")
        spark.stop()
        return

    start = None if args.full else datetime.now(timezone.utc).date() - timedelta(days=args.days - 1)
    table = f"tracewell.gold.{args.mart}"
    n = write(build(spark, args.mart, start, args.bootstrap), table, PARTITION[args.mart])
    log(f"{table}: wrote {n} rows ({'all partitions' if start is None else f'partitions from {start}'})")
    spark.stop()


if __name__ == "__main__":
    main()
