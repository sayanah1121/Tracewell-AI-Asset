"""Compact the streaming tables Flink writes, so the marts read a few large files instead of thousands of tiny ones.

Flink commits to Iceberg on every checkpoint (20 s) for each partition it touched, which leaves ~13 KB files:
3,325 of them for 44 MB of silver after one afternoon, and every mart opened all of them. Compaction only
rewrites files (no row changes) and commits optimistically, so it runs safely next to Flink's appends.

Manifests are left alone: Flink's merge-append already keeps their number bounded, and a concurrent
rewrite_manifests loses the race against its 20 s commits ("Deleted manifest ... could not be found").
"""
import argparse
from datetime import datetime, timedelta, timezone

from common import log, spark_session

TABLES = {"silver.fct_ai_events": "ts_event", "bronze.ai_events": "ts_event"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2, help="compact partitions from this many UTC days back")
    ap.add_argument("--full", action="store_true")
    args = ap.parse_args()
    spark = spark_session("compact-stream-tables")
    start = datetime.now(timezone.utc).date() - timedelta(days=args.days - 1)
    for table, ts_col in TABLES.items():
        where = "" if args.full else f", where => \"{ts_col} >= TIMESTAMP '{start} 00:00:00'\""
        r = spark.sql(f"""CALL tracewell.system.rewrite_data_files(table => '{table}'{where},
                          options => map('min-input-files', '2', 'partial-progress.enabled', 'true'))""").first()
        log(f"{table}: {r.rewritten_data_files_count} files -> {r.added_data_files_count} "
            f"({r.rewritten_bytes_count / 1e6:.1f} MB)")
    spark.stop()


if __name__ == "__main__":
    main()
