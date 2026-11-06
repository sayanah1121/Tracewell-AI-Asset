"""Phase 0 smoke test: Spark side of the shared Iceberg table.

Usage (spark-submit ... smoke_spark.py <mode> [engine]):
  init           drop + recreate tracewell.smoke.engine_writes and write 2 rows as "spark"
  append ENGINE  append 2 rows tagged with ENGINE (used by the Airflow DAG)
  verify         check that spark, flink and airflow all wrote to the table
"""
import sys

from pyspark.sql import SparkSession

TABLE = "tracewell.smoke.engine_writes"
EXPECTED_ENGINES = {"spark", "flink", "airflow"}


def append(spark: SparkSession, engine: str, first_id: int) -> None:
    spark.sql(f"""
        INSERT INTO {TABLE} VALUES
          ({first_id},     '{engine}', 'written by {engine}', current_timestamp()),
          ({first_id + 1}, '{engine}', 'written by {engine}', current_timestamp())
    """)
    print(f"[smoke] {engine}: appended 2 rows to {TABLE}")


def init(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS tracewell.smoke")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.sql(f"""
        CREATE TABLE {TABLE} (
          id         BIGINT,
          engine     STRING,
          note       STRING,
          written_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (engine)
        TBLPROPERTIES ('format-version' = '2')
    """)
    append(spark, "spark", 1)


def verify(spark: SparkSession) -> None:
    rows = spark.sql(f"SELECT engine, count(*) AS n FROM {TABLE} GROUP BY engine ORDER BY engine").collect()
    print("[smoke] rows per engine:", {r.engine: r.n for r in rows})

    snapshots = spark.sql(f"SELECT committed_at, operation, summary['added-records'] AS added FROM {TABLE}.snapshots ORDER BY committed_at")
    snapshots.show(truncate=False)

    missing = EXPECTED_ENGINES - {r.engine for r in rows}
    if missing:
        raise SystemExit(f"[smoke] FAILED: no rows from {sorted(missing)}")
    if snapshots.count() < len(EXPECTED_ENGINES):
        raise SystemExit("[smoke] FAILED: expected one Iceberg snapshot per engine write")
    print("[smoke] PASSED: Spark, Flink and Airflow all committed to the same Iceberg table")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "verify"
    spark = SparkSession.builder.appName(f"tracewell-smoke-{mode}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if mode == "init":
            init(spark)
        elif mode == "append":
            append(spark, sys.argv[2], first_id=100)
        elif mode == "verify":
            verify(spark)
        else:
            raise SystemExit(f"unknown mode {mode!r}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
