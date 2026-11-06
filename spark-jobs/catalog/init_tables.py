"""Create the Iceberg namespaces and tables the risk catalog needs (idempotent)."""
from common import log, spark_session

BRONZE_TABLES = [
    "pkg_metadata_raw",     # PyPI + npm package metadata
    "osv_advisories_raw",   # OSV query results per package version + vulnerability details
]

DDL = [
    "CREATE NAMESPACE IF NOT EXISTS tracewell.bronze",
    "CREATE NAMESPACE IF NOT EXISTS tracewell.silver",
    "CREATE NAMESPACE IF NOT EXISTS tracewell.ops",
    *[f"""
    CREATE TABLE IF NOT EXISTS tracewell.bronze.{t} (
      run_id      STRING    COMMENT 'ingestion run that fetched the record',
      source      STRING    COMMENT 'source system, e.g. pypi, npm, osv_vuln',
      record_key  STRING    COMMENT 'natural key within the source',
      payload     STRING    COMMENT 'raw JSON as returned by the API (trimmed of bulky fields)',
      fetched_at  TIMESTAMP,
      ingested_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(ingested_at))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    """ for t in BRONZE_TABLES],
    """
    CREATE TABLE IF NOT EXISTS tracewell.silver.asset_candidates (
      candidate_id     STRING,
      native_id        STRING     COMMENT 'asset id this record would have on its own, e.g. pkg:npm/<name>',
      source           STRING,
      category         STRING,
      name             STRING,
      norm_name        STRING,
      description      STRING,
      publisher        STRING,
      licence          STRING,
      repo             STRING     COMMENT 'normalised github owner/repo',
      package_key      STRING     COMMENT 'npm:<name> or pypi:<name>',
      declared_packages ARRAY<STRING> COMMENT 'package keys an MCP server declares',
      version          STRING,
      popularity       BIGINT     COMMENT 'npm downloads last month',
      last_updated     TIMESTAMP,
      archived         BOOLEAN,
      deprecated       BOOLEAN,
      is_remote        BOOLEAN    COMMENT 'MCP server hosted by a third party (remote endpoint)',
      pipeline_tag     STRING     COMMENT 'model task, e.g. text-generation (curated models only)',
      capability_text  STRING     COMMENT 'lowercased text used for capability signals',
      capability_tags  ARRAY<STRING> COMMENT 'curated capability tags (seed assets only)',
      popularity_hint  DOUBLE     COMMENT 'curated exposure prior (seed assets only)',
      vuln_ids         ARRAY<STRING>,
      vuln_max_severity STRING,
      malicious        BOOLEAN,
      built_at         TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (source)
    """,
    """
    CREATE TABLE IF NOT EXISTS tracewell.silver.asset_entity_map (
      candidate_id STRING,
      asset_id     STRING,
      match_rule   STRING COMMENT 'self | declared_package | shared_repo | fuzzy_name',
      built_at     TIMESTAMP
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS tracewell.silver.dim_asset_risk (
      asset_sk        BIGINT,
      asset_id        STRING,
      asset_name      STRING,
      category        STRING,
      publisher       STRING,
      licence         STRING,
      description     STRING,
      source_systems  ARRAY<STRING>,
      aliases         ARRAY<STRING>,
      version         STRING,
      popularity      BIGINT,
      capability      DECIMAL(3,1),
      exposure        DECIMAL(3,1),
      governance      DECIMAL(3,1),
      risk_score      DECIMAL(3,1),
      band            STRING,
      score_reasons   ARRAY<STRING>,
      sanctioned      BOOLEAN,
      valid_from      TIMESTAMP,
      valid_to        TIMESTAMP,
      is_current      BOOLEAN,
      row_hash        STRING
    ) USING iceberg
    PARTITIONED BY (category)
    TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read')
    """,
    """
    CREATE TABLE IF NOT EXISTS tracewell.ops.pipeline_state (
      component  STRING,
      key        STRING,
      value      STRING,
      updated_at TIMESTAMP
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS tracewell.ops.entity_resolution_metrics (
      run_at        TIMESTAMP,
      evaluated_on  STRING,
      true_pos      BIGINT,
      false_pos     BIGINT,
      false_neg     BIGINT,
      precision_score DOUBLE,
      recall_score    DOUBLE
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS tracewell.ops.dq_results (
      run_at  TIMESTAMP,
      suite   STRING,
      check_name STRING,
      passed  BOOLEAN,
      detail  STRING
    ) USING iceberg
    """,
]


def main() -> None:
    spark = spark_session("init-tables")
    for stmt in DDL:
        spark.sql(stmt)
    for ns in ("bronze", "silver", "ops"):
        tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN tracewell.{ns}").collect()]
        log(f"tracewell.{ns}: {', '.join(sorted(tables))}")
    spark.stop()


if __name__ == "__main__":
    main()
