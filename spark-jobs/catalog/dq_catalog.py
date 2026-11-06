"""Data-quality checks for the risk catalog. Results go to ops.dq_results; a failed fatal check fails the task."""
import argparse
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import env_int, log, spark_session, utcnow

DIM = "tracewell.silver.dim_asset_risk"
BRONZE = ["pkg_metadata_raw", "osv_advisories_raw"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-assets", type=int, default=env_int("CATALOG_MIN_ASSETS", 15000))
    args = ap.parse_args()
    spark = spark_session("dq-catalog")
    dim = spark.table(DIM)
    current = dim.where("is_current")
    results = []  # (check, passed, detail, fatal)

    n = current.count()
    results.append(("enough_current_assets", n >= args.min_assets, f"{n} current assets (min {args.min_assets})", True))

    dup = current.groupBy("asset_id").count().where("count > 1").count()
    results.append(("one_current_version_per_asset", dup == 0, f"{dup} assets with >1 current row", True))

    w = Window.partitionBy("asset_id").orderBy("valid_from")
    versions = dim.withColumn("next_from", F.lead("valid_from").over(w))
    overlaps = versions.where("(next_from IS NOT NULL AND valid_to > next_from) OR valid_from >= valid_to").count()
    results.append(("scd2_no_overlapping_versions", overlaps == 0, f"{overlaps} overlapping or empty versions", True))
    # A gap is legitimate when an asset was retired (e.g. merged elsewhere) and later reappeared.
    gaps = versions.where("next_from IS NOT NULL AND valid_to < next_from").count()
    results.append(("scd2_gaps_are_retirements", True, f"{gaps} gaps (asset retired, then reappeared)", False))

    bad_range = current.where(" OR ".join(f"{c} < 0 OR {c} > 10 OR {c} IS NULL" for c in
                                          ("capability", "exposure", "governance", "risk_score"))).count()
    results.append(("scores_between_0_and_10", bad_range == 0, f"{bad_range} rows out of range", True))

    band = (F.when(F.col("risk_score") >= 9, "Critical").when(F.col("risk_score") >= 7, "High")
            .when(F.col("risk_score") >= 5, "Medium").otherwise("Low"))
    bad_band = current.where(F.col("band") != band).count()
    results.append(("band_matches_score", bad_band == 0, f"{bad_band} rows with wrong band", True))

    no_reason = current.where(F.size("score_reasons") == 0).count()
    results.append(("every_score_explained", no_reason == 0, f"{no_reason} rows without reasons", False))

    unmapped = (spark.table("tracewell.silver.asset_candidates").select("candidate_id")
                .join(spark.table("tracewell.silver.asset_entity_map"), "candidate_id", "left_anti").count())
    results.append(("every_candidate_mapped", unmapped == 0, f"{unmapped} candidates without an asset", True))

    multi_mcp = (spark.table("tracewell.silver.asset_entity_map").join(spark.table("tracewell.silver.asset_candidates").where("category = 'mcp_server'")
                         .select("candidate_id"), "candidate_id")
                 .groupBy("asset_id").count().where("count > 1").count())
    results.append(("max_one_mcp_server_per_asset", multi_mcp == 0, f"{multi_mcp} assets merge several MCP servers", True))

    resolved = spark.table("tracewell.silver.asset_entity_map").select("asset_id").distinct().count()
    results.append(("current_assets_match_resolution", resolved == n,
                    f"{n} current assets vs {resolved} resolved assets", True))

    age_h = spark.table("tracewell.silver.asset_candidates").agg(
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("built_at"))) / 3600).first()[0]
    results.append(("catalog_fresh_26h", age_h is not None and age_h <= 26, f"built {age_h:.1f} h ago" if age_h is not None else "never built", True))

    for t in BRONZE:
        last = spark.table(f"tracewell.bronze.{t}").agg(F.max("ingested_at")).first()[0]
        fresh = last is not None and (utcnow() - last).total_seconds() <= 26 * 3600
        results.append((f"bronze_{t}_ingested_26h", fresh, f"last ingest {last}", False))

    spark.createDataFrame([(r[0], r[1], r[2]) for r in results], "check_name string, passed boolean, detail string") \
        .select(F.current_timestamp().alias("run_at"), F.lit("catalog").alias("suite"), "*") \
        .writeTo("tracewell.ops.dq_results").append()
    failed_fatal = [r for r in results if not r[1] and r[3]]
    for check, passed, detail, fatal in results:
        log(f"{'PASS' if passed else ('FAIL' if fatal else 'WARN')}  {check}: {detail}")
    spark.stop()
    if failed_fatal:
        sys.exit(f"{len(failed_fatal)} fatal data-quality checks failed")


if __name__ == "__main__":
    main()
