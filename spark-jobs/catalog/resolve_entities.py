"""Normalise every source into silver.asset_candidates and resolve candidates into canonical assets.

Sources: PyPI/npm package metadata (with OSV advisories) and the curated seed of AI services, agents,
extensions, MCP servers and local models (seed/curated_assets.csv). One real-world asset often appears
twice: a curated MCP server and the npm or PyPI package that ships it. Resolution links MCP servers and
packages with three rules (strongest first), then takes connected components:
  1. declared_package - the curated entry lists the package identifier (ground truth)
  2. shared_repo      - same GitHub repo, with a monorepo guard: only when that repo hosts at most one
                        MCP server, one npm and one PyPI package (e.g. modelcontextprotocol/servers
                        hosts many servers and must not merge them)
  3. fuzzy_name       - normalised names within Levenshtein similarity >= 0.9, only when the name is
                        unique on both sides
Rules 2 and 3 are scored against rule 1 and written to ops.entity_resolution_metrics.
"""
import csv
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import latest_bronze, log, spark_session
from transforms import github_repo, norm_name, npm_key, pypi_key

SEED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seed")
SEVERITY_RANK = {"CRITICAL": 4, "HIGH": 3, "MODERATE": 2, "MEDIUM": 2, "LOW": 1}
CANDIDATE_COLS = [
    "candidate_id", "native_id", "source", "category", "name", "norm_name", "description", "publisher",
    "licence", "repo", "package_key", "declared_packages", "version", "popularity",
    "last_updated", "archived", "deprecated", "is_remote", "pipeline_tag", "capability_text",
    "capability_tags", "popularity_hint", "vuln_ids", "vuln_max_severity", "malicious",
]
NULLS = {
    "repo": "string", "package_key": "string", "declared_packages": "array<string>", "version": "string",
    "archived": "boolean", "deprecated": "boolean", "is_remote": "boolean", "pipeline_tag": "string", "capability_tags": "array<string>",
    "popularity_hint": "double", "vuln_ids": "array<string>", "vuln_max_severity": "string",
    "malicious": "boolean", "popularity": "bigint", "last_updated": "timestamp", "licence": "string",
    "description": "string", "publisher": "string",
}


def j(path: str):
    return F.get_json_object("payload", path)


def ts(col):
    return F.to_timestamp(col)


def nullif(col, value):
    """NULL when col equals value (F.nullif fails on unresolved columns in PySpark 3.5)."""
    return F.when(col != value, col)


def conform(df: DataFrame) -> DataFrame:
    for c, t in NULLS.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(t))
    return df.select(*CANDIDATE_COLS)


# ───────────────────────────── per-source candidates ─────────────────────────────

def package_candidates(spark: SparkSession) -> DataFrame:
    raw = latest_bronze(spark, "tracewell.bronze.pkg_metadata_raw").where(~F.col("payload").contains('"not_found":true'))
    pypi_urls = F.map_values(F.from_json(j("$.info.project_urls"), "map<string,string>"))
    pypi_repo = F.element_at(F.filter(F.transform(pypi_urls, github_repo), lambda r: r.isNotNull()), 1)
    pypi = raw.where("source = 'pypi'").select(
        F.concat(F.lit("pypi:"), F.col("record_key")).alias("candidate_id"),
        F.concat(F.lit("pkg:pypi/"), F.col("record_key")).alias("native_id"),
        F.col("source"), F.lit("package").alias("category"),
        F.coalesce(j("$.info.name"), F.col("record_key")).alias("name"),
        j("$.info.summary").alias("description"),
        F.lower(F.coalesce(j("$.info.author"), j("$.info.maintainer"))).alias("publisher"),
        F.coalesce(nullif(j("$.info.license_expression"), F.lit("")),
                   F.when(F.length(j("$.info.license")) < 40, j("$.info.license"))).alias("licence"),
        pypi_repo.alias("repo"),
        pypi_key(F.col("record_key")).alias("package_key"),
        j("$.info.version").alias("version"),
        ts(j("$.latest_upload")).alias("last_updated"),
        j("$.info.classifiers").contains("Development Status :: 7 - Inactive").alias("deprecated"),
        F.lower(F.concat_ws(" ", F.col("record_key"), j("$.info.summary"), j("$.info.keywords"))).alias("capability_text"),
    )
    npm_lic = F.coalesce(j("$.license.type"), j("$.license"))
    npm = raw.where("source = 'npm'").select(
        F.concat(F.lit("npm:"), F.col("record_key")).alias("candidate_id"),
        F.concat(F.lit("pkg:npm/"), F.col("record_key")).alias("native_id"),
        F.col("source"), F.lit("package").alias("category"),
        F.col("record_key").alias("name"),
        j("$.description").alias("description"),
        F.lower(F.coalesce(F.regexp_extract("record_key", r"^@([^/]+)/", 1), j("$.author.name"))).alias("publisher"),
        npm_lic.alias("licence"),
        github_repo(F.coalesce(j("$.repository.url"), j("$.repository"))).alias("repo"),
        npm_key(F.col("record_key")).alias("package_key"),
        j("$.version").alias("version"),
        j("$.downloads_monthly").cast("bigint").alias("popularity"),
        ts(j("$.date")).alias("last_updated"),
        (j("$.deprecated").isNotNull() & (j("$.deprecated") != "false")).alias("deprecated"),
        F.lower(F.concat_ws(" ", F.col("record_key"), j("$.description"),
                            F.array_join(F.from_json(j("$.keywords"), "array<string>"), " "))).alias("capability_text"),
    )
    return conform(pypi.unionByName(npm, allowMissingColumns=True)
                   .withColumn("norm_name", norm_name(F.col("name"))))


def seed_candidates(spark: SparkSession) -> DataFrame:
    with open(os.path.join(SEED_DIR, "curated_assets.csv"), newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    df = spark.createDataFrame(rows)
    declared = F.transform(F.filter(F.split("packages", ";"), lambda k: k != ""),
                           lambda k: F.when(k.startswith("pypi:"), pypi_key(F.substring_index(k, ":", -1)))
                           .otherwise(npm_key(F.substring_index(k, ":", -1))))
    return conform(df.select(
        F.concat(F.lit("seed:"), "asset_id").alias("candidate_id"),
        F.col("asset_id").alias("native_id"),
        F.lit("seed").alias("source"), F.col("category"), F.col("name"),
        norm_name(F.col("name")).alias("norm_name"),
        F.col("description"), F.col("publisher"), nullif(F.col("licence"), F.lit("")).alias("licence"),
        nullif(F.col("repo"), F.lit("")).alias("repo"),
        F.when(F.size(declared) > 0, declared).alias("declared_packages"),
        nullif(F.col("pipeline_tag"), F.lit("")).alias("pipeline_tag"),
        (F.col("is_remote") == "true").alias("is_remote"),
        F.filter(F.split("capability_tags", ";"), lambda t: t != "").alias("capability_tags"),
        F.col("popularity_hint").cast("double").alias("popularity_hint"),
        F.current_timestamp().alias("last_updated"),
        F.lower(F.concat_ws(" ", "name", "description")).alias("capability_text"),
    ))


# ───────────────────────────── enrichment ─────────────────────────────

def enrich(spark: SparkSession, cands: DataFrame) -> DataFrame:
    osv = latest_bronze(spark, "tracewell.bronze.osv_advisories_raw")
    sev_rank = F.create_map(*[x for k, v in SEVERITY_RANK.items() for x in (F.lit(k), F.lit(v))])
    vulns = osv.where("source = 'osv_vuln'").select(
        F.col("record_key").alias("vuln_id"),
        F.coalesce(sev_rank[F.upper(j("$.severity"))], F.lit(1)).alias("rank"),
        (j("$.malicious") == "true").alias("is_mal"),
        j("$.withdrawn").isNotNull().alias("withdrawn"))
    queries = osv.where("source = 'osv_query'").select(
        F.concat(j("$.ecosystem"), F.lit(":"), j("$.name")).alias("osv_pkg"),
        j("$.version").alias("osv_version"),
        F.explode(F.from_json(j("$.vuln_ids"), "array<string>")).alias("vuln_id"))
    rank_name = F.create_map(*[x for k, v in {"CRITICAL": 4, "HIGH": 3, "MODERATE": 2, "LOW": 1}.items()
                               for x in (F.lit(v), F.lit(k))])
    pkg_vulns = (queries.join(vulns, "vuln_id", "left").where(~F.coalesce("withdrawn", F.lit(False)))
                 .groupBy("osv_pkg", "osv_version")
                 .agg(F.collect_set("vuln_id").alias("osv_ids"),
                      rank_name[F.max(F.coalesce("rank", F.lit(1)))].alias("osv_sev"),
                      F.max(F.coalesce("is_mal", F.lit(False))).alias("osv_mal")))
    pkg_name_key = F.regexp_replace(F.col("package_key"), r"^(pypi|npm):", "")

    out = cands.join(pkg_vulns, (F.concat(F.col("source"), F.lit(":"), pkg_name_key) == F.col("osv_pkg"))
                     & (F.col("version") == F.col("osv_version")), "left")
    return out.select(
        *[c for c in CANDIDATE_COLS if c not in ("licence", "publisher", "vuln_ids", "vuln_max_severity", "malicious")],
        nullif(F.col("licence"), F.lit("")).alias("licence"),
        nullif(F.col("publisher"), F.lit("")).alias("publisher"),
        F.coalesce("osv_ids", "vuln_ids").alias("vuln_ids"),
        F.coalesce("osv_sev", "vuln_max_severity").alias("vuln_max_severity"),
        F.coalesce("osv_mal", "malicious", F.lit(False)).alias("malicious"),
    ).select(*CANDIDATE_COLS)


# ───────────────────────────── entity resolution ─────────────────────────────

def edges_declared(c: DataFrame) -> DataFrame:
    mcp = c.where("category = 'mcp_server'").select("candidate_id", F.explode("declared_packages").alias("package_key"))
    pkg = c.where("category = 'package'").select(F.col("candidate_id").alias("dst"), "package_key")
    return mcp.join(pkg, "package_key").select(F.col("candidate_id").alias("src"), "dst", F.lit("declared_package").alias("rule"))


def edges_shared_repo(c: DataFrame) -> DataFrame:
    nodes = c.where("category IN ('mcp_server', 'package') AND repo IS NOT NULL")         .withColumn("kind", F.when(F.col("category") == "mcp_server", "mcp").otherwise(F.col("source")))
    guard = (nodes.groupBy("repo").agg(F.count("*").alias("n"), F.countDistinct("kind").alias("s"),
                                       F.sum(F.when(F.col("kind") == "mcp", 1).otherwise(0)).alias("mcp"))
             .where("n = s AND mcp = 1 AND n > 1"))  # at most one node per source, exactly one MCP server
    members = nodes.join(guard.select("repo"), "repo").select("repo", "candidate_id", "kind")
    a, b = members.alias("a"), members.alias("b")
    return (a.join(b, (F.col("a.repo") == F.col("b.repo")) & (F.col("a.kind") == "mcp")
                   & (F.col("b.kind") != "mcp"))
            .select(F.col("a.candidate_id").alias("src"), F.col("b.candidate_id").alias("dst"),
                    F.lit("shared_repo").alias("rule")))


def edges_fuzzy(c: DataFrame) -> DataFrame:
    def unique_side(df):
        once = df.groupBy("norm_name").count().where("count = 1").select("norm_name")
        return df.join(once, "norm_name").withColumn("block", F.substring("norm_name", 1, 3))
    base = c.where("norm_name IS NOT NULL AND length(norm_name) >= 4")
    mcp = unique_side(base.where("category = 'mcp_server'").select("candidate_id", "norm_name", "publisher")).alias("m")
    pkg = unique_side(base.where("category = 'package'")
                      .select("candidate_id", "norm_name", "publisher",
                              F.split("repo", "/")[0].alias("owner"),
                              F.lower("name").contains("mcp").alias("is_mcp_pkg"))).alias("p")
    sim = 1 - F.levenshtein(F.col("m.norm_name"), F.col("p.norm_name")) / \
        F.greatest(F.length(F.col("m.norm_name")), F.length(F.col("p.norm_name")))
    # A similar name alone is not enough ('gen-ai' MCP server vs the @google/genai SDK): the package
    # must be an MCP package, or share a publisher / repo owner with the server.
    plausible = F.col("p.is_mcp_pkg") | (F.col("m.publisher") == F.col("p.publisher")) \
        | (F.col("m.publisher") == F.col("p.owner"))
    return (mcp.join(pkg, F.col("m.block") == F.col("p.block")).where((sim >= 0.9) & plausible)
            .select(F.col("m.candidate_id").alias("src"), F.col("p.candidate_id").alias("dst"),
                    F.lit("fuzzy_name").alias("rule")))


RULE_RANK = {"declared_package": 1, "shared_repo": 2, "fuzzy_name": 3}


def rule_rank(rule):
    return F.create_map(*[x for k, v in RULE_RANK.items() for x in (F.lit(k), F.lit(v))])[rule]


def best_edge_per_package(edges: DataFrame) -> DataFrame:
    """Every edge is MCP server -> package. Letting each package keep only its best edge means a
    component never holds two MCP servers (no chaining: server A - package - server B)."""
    best = Window.partitionBy("dst").orderBy(rule_rank(F.col("rule")), "src")
    return edges.withColumn("_rn", F.row_number().over(best)).where("_rn = 1").drop("_rn")


def connected_components(nodes: DataFrame, edges: DataFrame, max_iter: int = 20) -> DataFrame:
    """Label propagation: every node takes the smallest label among itself and its neighbours.

    Labels are '<priority><candidate_id>' so the winning label is the highest-priority member
    (MCP server, then package), which becomes the canonical asset.
    """
    prio = F.when(F.col("category") == "mcp_server", "1").when(F.col("category") == "package", "2").otherwise("3")
    labels = nodes.select("candidate_id", F.concat(prio, F.col("candidate_id")).alias("label")).localCheckpoint()
    und = edges.select("src", "dst").unionByName(edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"))).distinct().cache()
    for i in range(max_iter):
        neighbour = (und.join(labels.withColumnRenamed("candidate_id", "src"), "src")
                     .groupBy(F.col("dst").alias("candidate_id")).agg(F.min("label").alias("n_label")))
        new = (labels.join(neighbour, "candidate_id", "left")
               .select("candidate_id", F.least("label", F.coalesce("n_label", "label")).alias("label"))
               .localCheckpoint())
        changed = new.join(labels.withColumnRenamed("label", "old"), "candidate_id").where("label != old").count()
        labels = new
        log(f"connected components: iteration {i + 1}, {changed} labels changed")
        if changed == 0:
            break
    return labels


def evaluate(edges: DataFrame) -> tuple[int, int, int]:
    """Score heuristic rules (shared_repo, fuzzy_name) against declared links as ground truth.

    Only MCP servers that declare at least one package are judged, so the comparison is fair.
    """
    truth = edges.where("rule = 'declared_package'").select("src", "dst").distinct()
    judged = truth.select("src").distinct()
    pred = edges.where("rule != 'declared_package'").select("src", "dst").distinct().join(judged, "src")
    tp = pred.join(truth, ["src", "dst"]).count()
    fp = pred.count() - tp
    fn = truth.join(pred, ["src", "dst"], "left_anti").count()
    return tp, fp, fn


def main() -> None:
    spark = spark_session("resolve-entities")
    raw = package_candidates(spark).unionByName(seed_candidates(spark))
    cands = enrich(spark, raw).dropDuplicates(["candidate_id"]).localCheckpoint()
    counts = {r.source: r["count"] for r in cands.groupBy("source").count().collect()}
    log(f"candidates by source: {counts}")

    linkable = cands.where("category IN ('mcp_server', 'package')")
    edges = edges_declared(linkable).unionByName(edges_shared_repo(linkable)).unionByName(edges_fuzzy(linkable)).cache()
    log(f"edges by rule: { {r.rule: r['count'] for r in edges.groupBy('rule').count().collect()} }")

    tp, fp, fn = evaluate(edges)
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    log(f"heuristic matching vs declared links: tp={tp} fp={fp} fn={fn} precision={precision} recall={recall}")
    spark.createDataFrame([("shared_repo+fuzzy_name vs declared_package", tp, fp, fn, precision, recall)],
                          "evaluated_on string, true_pos long, false_pos long, false_neg long, "
                          "precision_score double, recall_score double") \
        .select(F.current_timestamp().alias("run_at"), "*").writeTo("tracewell.ops.entity_resolution_metrics").append()

    edges = best_edge_per_package(edges).cache()
    log(f"edges kept after one-server-per-package pruning: {edges.count()}")

    labels = connected_components(cands.select("candidate_id", "source", "category"), edges)
    canon = cands.select(F.col("candidate_id").alias("canon_id"), F.col("native_id").alias("asset_id"))
    node_rule = (edges.select(F.col("dst").alias("candidate_id"), "rule")
                 .unionByName(edges.select(F.col("src").alias("candidate_id"), "rule"))
                 .groupBy("candidate_id").agg(F.min_by("rule", rule_rank(F.col("rule"))).alias("rule")))
    mapping = (labels.withColumn("canon_id", F.expr("substring(label, 2)"))
               .join(canon, "canon_id")
               .join(node_rule, "candidate_id", "left")
               .select("candidate_id", "asset_id",
                       F.when(F.col("candidate_id") == F.col("canon_id"), "self")
                        .otherwise(F.col("rule")).alias("match_rule"),
                       F.current_timestamp().alias("built_at")))

    cands.withColumn("built_at", F.current_timestamp()) \
        .writeTo("tracewell.silver.asset_candidates").overwrite(F.lit(True))
    mapping.writeTo("tracewell.silver.asset_entity_map").overwrite(F.lit(True))

    m = spark.table("tracewell.silver.asset_entity_map")
    log(f"{m.count()} candidates -> {m.select('asset_id').distinct().count()} assets; "
        f"merged by rule: { {r.match_rule: r['count'] for r in m.groupBy('match_rule').count().collect()} }")
    spark.stop()


if __name__ == "__main__":
    main()
