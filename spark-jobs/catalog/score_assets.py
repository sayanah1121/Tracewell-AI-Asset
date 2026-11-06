"""Score every resolved asset with Tracewell and merge the result into silver.dim_asset_risk (SCD Type 2).

History rules (hybrid SCD 1 + 2):
  * scored attributes (capability, exposure, governance, score, band, reasons, sanctioned, licence)
    are hashed into row_hash; when the hash changes, the current row is closed (valid_to = run time)
    and a new version is inserted;
  * descriptive attributes (name, description, version, popularity, aliases, sources) are updated
    in place on the current row, because a download count changing is not a risk event;
  * assets no longer produced by entity resolution are retired (current row closed, no new version).
"""
import argparse
import csv
import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import log, spark_session, utcnow
from transforms import score_assets

SEED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seed")
DIM = "tracewell.silver.dim_asset_risk"
SCORED = ["capability", "exposure", "governance", "risk_score", "band", "sanctioned", "licence", "score_reasons"]
TYPE1 = ["asset_name", "category", "publisher", "description", "source_systems", "aliases", "version", "popularity"]
SEV = {"CRITICAL": 4, "HIGH": 3, "MODERATE": 2, "LOW": 1}


def read_seed_column(path: str, column: str) -> list[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return [r[column].strip().lower() for r in csv.DictReader(f) if r[column].strip()]


def build_assets(spark, allow_ids: list[str], trusted: list[str]):
    c = spark.table("tracewell.silver.asset_candidates").join(
        spark.table("tracewell.silver.asset_entity_map").select("candidate_id", "asset_id"), "candidate_id")
    canon = (F.col("native_id") == F.col("asset_id")).cast("int")

    def pick(col: str):
        """Value from the canonical candidate if it has one, else from any member that does."""
        return F.max_by(F.col(col), F.when(F.col(col).isNotNull(), canon * 2 + 1).otherwise(0)).alias(col)

    sev_rank = F.create_map(*[x for k, v in SEV.items() for x in (F.lit(k), F.lit(v))])
    sev_name = F.create_map(*[x for k, v in SEV.items() for x in (F.lit(v), F.lit(k))])
    agg = c.groupBy("asset_id").agg(
        pick("name"), pick("category"), pick("publisher"), pick("licence"), pick("description"),
        pick("version"), pick("pipeline_tag"), pick("repo"),
        F.array_sort(F.collect_set("source")).alias("source_systems"),
        F.array_sort(F.array_distinct(F.array_compact(F.flatten(F.array(
            F.collect_list("native_id"), F.collect_list(F.lower("name")),
            F.collect_list("package_key"), F.collect_list("repo")))))).alias("aliases"),
        F.concat_ws(" ", F.collect_list("capability_text")).alias("capability_text"),
        F.array_distinct(F.flatten(F.collect_list(F.coalesce("capability_tags", F.array().cast("array<string>"))))).alias("capability_tags"),
        F.max("popularity_hint").alias("popularity_hint"),
        F.max("popularity").alias("popularity"),
        F.max(F.col("repo").isNotNull()).alias("has_repo"),
        F.max("is_remote").alias("is_remote"), F.max("archived").alias("archived"),
        F.max("deprecated").alias("deprecated"), F.max("last_updated").alias("last_updated"),
        F.max("malicious").alias("malicious"),
        sev_name[F.max(sev_rank[F.col("vuln_max_severity")])].alias("vuln_max_severity"),
    )
    pct = F.when(F.col("popularity").isNotNull(),
                 F.percent_rank().over(Window.partitionBy("category", F.col("popularity").isNotNull())
                                       .orderBy("popularity")))
    trusted_arr = F.array(*[F.lit(t) for t in trusted])
    owner = F.split(F.col("repo"), "/")[0]
    return (agg.withColumn("pop_pct", pct)
            .withColumn("publisher_trusted", F.array_contains(trusted_arr, F.col("publisher"))
                        | F.coalesce(F.array_contains(trusted_arr, owner), F.lit(False)))
            .withColumn("sanctioned", F.arrays_overlap(F.col("aliases"), F.array(*[F.lit(a) for a in allow_ids]))
                        | F.array_contains(F.array(*[F.lit(a) for a in allow_ids]), F.col("asset_id"))))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--allow-list", default=os.path.join(SEED_DIR, "allow_list.csv"))
    args = ap.parse_args()

    spark = spark_session("score-assets")
    run_ts = utcnow().replace(microsecond=0)
    allow_ids = read_seed_column(args.allow_list, "asset_id")
    trusted = read_seed_column(os.path.join(SEED_DIR, "trusted_publishers.csv"), "publisher")

    scored = score_assets(build_assets(spark, allow_ids, trusted))
    staged = scored.select(
        F.xxhash64("asset_id", F.lit(run_ts.isoformat())).alias("asset_sk"),
        "asset_id", F.col("name").alias("asset_name"), "category", "publisher", "licence", "description",
        "source_systems", "aliases", "version", "popularity", "capability", "exposure", "governance",
        "risk_score", "band", "score_reasons", "sanctioned",
        F.lit(run_ts).alias("valid_from"), F.lit("9999-12-31 00:00:00").cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SCORED[:-1]],
                           F.array_join("score_reasons", ";")), 256).alias("row_hash"),
    ).localCheckpoint()
    staged.createOrReplaceTempView("staged")
    log(f"scored {staged.count()} assets")

    cols = staged.columns
    type1_changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in TYPE1)
    spark.sql(f"""
        MERGE INTO {DIM} t
        USING (
          SELECT s.asset_id AS merge_key, s.* FROM staged s
          UNION ALL
          -- a second copy of every changed asset, with no merge key, becomes the new version
          SELECT NULL AS merge_key, s.* FROM staged s
          JOIN {DIM} c ON c.asset_id = s.asset_id AND c.is_current AND c.row_hash <> s.row_hash
        ) s
        ON t.asset_id = s.merge_key AND t.is_current
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN
          UPDATE SET t.valid_to = s.valid_from, t.is_current = false
        WHEN MATCHED AND ({type1_changed}) THEN
          UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in TYPE1)}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})
        -- assets that resolution no longer produces (e.g. a package now merged into its MCP server) are retired
        WHEN NOT MATCHED BY SOURCE AND t.is_current THEN
          UPDATE SET t.valid_to = timestamp'{run_ts:%Y-%m-%d %H:%M:%S}', t.is_current = false
    """)

    dim = spark.table(DIM)
    new_versions = dim.where(F.col("valid_from") == F.lit(run_ts)).count()
    closed = dim.where(F.col("valid_to") == F.lit(run_ts)).count()
    log(f"merge done: {new_versions} versions inserted, {closed} versions closed")
    log("current assets by band: " + str({r.band: r["count"] for r in
                                          dim.where("is_current").groupBy("band").count().collect()}))
    spark.stop()


if __name__ == "__main__":
    main()
