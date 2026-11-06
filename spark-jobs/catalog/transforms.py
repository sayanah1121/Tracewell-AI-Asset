"""Pure Spark column logic for the risk catalog: normalisation, capability signals and Tracewell risk scoring.

Everything here is a Column expression (no Python UDFs), so it runs inside the JVM and can be
unit-tested with a local SparkSession (see tests/catalog).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# ───────────────────────────── normalisation ─────────────────────────────

GITHUB_RE = r"github\.com[/:]+([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:\.git)?(?:[/#?].*)?$"


def github_repo(url: Column) -> Column:
    """'git+https://github.com/Owner/Repo.git#main' -> 'owner/repo'; NULL when not a GitHub URL."""
    owner = F.regexp_extract(url, GITHUB_RE, 1)
    repo = F.regexp_extract(url, GITHUB_RE, 2)
    return F.when((owner != "") & (repo != ""), F.lower(F.concat_ws("/", owner, repo)))


def norm_name(name: Column) -> Column:
    """Comparable short name: drop scope / reverse-DNS prefix and MCP affixes, keep [a-z0-9].

    '@modelcontextprotocol/server-postgres' -> 'postgres'
    'io.github.acme/postgres-mcp-server'    -> 'postgres'
    'mcp-server-postgres'                   -> 'postgres'
    """
    n = F.lower(F.trim(name))
    n = F.regexp_replace(n, r"^.*/", "")                                   # scope or namespace
    n = F.regexp_replace(n, r"^(mcp[-_]?server[-_]|server[-_]|mcp[-_])", "")
    n = F.regexp_replace(n, r"([-_]mcp[-_]server|[-_]mcp|[-_]server)$", "")
    n = F.regexp_replace(n, r"[^a-z0-9]", "")
    return F.when(F.length(n) > 0, n)


def pypi_key(name: Column) -> Column:
    return F.concat(F.lit("pypi:"), F.lower(F.regexp_replace(name, r"[-_.]+", "-")))


def npm_key(name: Column) -> Column:
    return F.concat(F.lit("npm:"), F.lower(F.trim(name)))


PERMISSIVE = r"^(mit|apache[- ]?2(\.0)?|bsd.*|isc|mpl.*|unlicense|cc0.*|cc-by-4\.0|openrail.*|llama[0-9.]*|gemma|zlib|python-2\.0|0bsd)$"
NON_COMMERCIAL = r"(^|[^a-z])(nc|non-commercial|noncommercial|research)([^a-z]|$)"


def licence_class(lic: Column) -> Column:
    """permissive | restricted | unknown."""
    l = F.lower(F.trim(lic))
    return (F.when(l.isNull() | (l == "") | l.isin("other", "unknown", "none", "proprietary")
                   | l.startswith("see license"), "unknown")
             .when(l.rlike(NON_COMMERCIAL), "restricted")
             .when(l.rlike(PERMISSIVE), "permissive")
             .otherwise("restricted"))


# ───────────────────────────── capability signals ─────────────────────────────
# (tag, regex over capability_text, points, label). Curated seed assets carry the tag directly.
CAPABILITY_SIGNALS = [
    ("code_exec",   r"\b(shell|terminal|bash|powershell|exec(ute|ution)?|subprocess|run(s|ning)? (code|commands?)|command line|ssh|sandbox|kubectl|docker)\b", 4.0, "can execute code or shell commands"),
    ("credentials", r"\b(credentials?|secrets?|passwords?|oauth|vault|api[- ]keys?|private keys?|wallets?|payments?|banking)\b", 2.0, "handles credentials or payments"),
    ("file_write",  r"\b(write|writes|delete|deletes|insert|update records|modify|upload|deploy|commit|push|create (issues?|files?|records?|pull requests?))\b", 2.5, "can write or delete data"),
    ("network",     r"\b(https?|fetch|browser|browse|scrap(e|ing)|crawl|e-?mail|slack|webhooks?|web search|websites?)\b", 2.0, "has outbound network access"),
    ("file_read",   r"\b(files?|filesystem|database|sql|postgres(ql)?|mysql|mongo(db)?|sqlite|google drive|documents?|notion|s3|storage|query|queries)\b", 1.5, "reads files or data stores"),
    ("autonomy",    r"\b(agents?|agentic|autonomous(ly)?|automat(e|es|ion)|orchestrat(e|ion|or))\b", 1.5, "acts autonomously"),
]

CATEGORY_BASE = {  # starting capability before signals; MCP servers and agents exist to give AI callable tools
    "model": 2.0, "dataset": 1.0, "mcp_server": 3.0, "package": 1.0,
    "ai_service": 2.0, "agent": 3.0, "extension": 2.0,
}
WEIGHTS = {"capability": 0.35, "exposure": 0.25, "governance": 0.40}


def _has(df_text: Column, tags: Column, tag: str, regex: str) -> Column:
    return F.coalesce(F.array_contains(tags, tag), F.lit(False)) | F.coalesce(df_text.rlike(regex), F.lit(False))


def _reason(cond: Column, points: float, label: str) -> Column:
    sign = "+" if points >= 0 else ""
    return F.when(cond, F.struct(F.lit(abs(points)).alias("w"), F.lit(f"{label} ({sign}{points:g})").alias("label")))


def score_assets(df: DataFrame, today: Column | None = None) -> DataFrame:
    """Add capability, exposure, governance, risk_score, band and score_reasons.

    Expects one row per asset with columns: category, capability_text, capability_tags, pipeline_tag,
    pop_pct (0-1 popularity percentile within category, NULL if unknown), popularity_hint, is_remote,
    licence, has_repo, publisher_trusted, last_updated, archived, deprecated, vuln_max_severity,
    malicious, sanctioned.
    """
    today = today if today is not None else F.current_timestamp()
    text = F.lower(F.coalesce(F.col("capability_text"), F.lit("")))
    tags = F.coalesce(F.col("capability_tags"), F.array().cast("array<string>"))

    # Capability ─ what it can do
    cap_terms, reasons = [], []
    base = F.lit(None).cast("double")
    for cat, pts in CATEGORY_BASE.items():
        base = F.when(F.col("category") == cat, F.lit(pts)).otherwise(base)
    base = F.coalesce(base, F.lit(1.0))
    gen = (F.col("category") == "model") & F.col("pipeline_tag").isin("text-generation", "image-text-to-text", "any-to-any")
    cap_terms.append(F.when(gen, 1.0).otherwise(0.0))
    reasons.append(_reason(gen, 1.0, "generative model"))
    for tag, regex, pts, label in CAPABILITY_SIGNALS:
        cond = _has(text, tags, tag, regex) & ~F.col("category").isin("model", "dataset")
        cap_terms.append(F.when(cond, pts).otherwise(0.0))
        reasons.append(_reason(cond, pts, label))
    capability = F.least(F.lit(10.0), base + sum(cap_terms))

    # Exposure ─ how far it reaches (static prior; fleet telemetry adds the dynamic part in Phase 2)
    pop = F.coalesce(F.col("popularity_hint"), F.round(F.col("pop_pct") * 10) / 2, F.lit(0.0))
    remote = F.coalesce(F.col("is_remote"), F.lit(False))
    widely_used = pop >= 4.0
    exposure = F.least(F.lit(10.0), pop + F.when(remote, 1.5).otherwise(0.0))
    reasons.append(_reason(remote, 1.5, "hosted remotely by a third party"))
    reasons.append(F.when(widely_used, F.struct(pop.alias("w"),
                                                F.format_string("widely used (+%.1f)", pop).alias("label"))))

    # Governance ─ how trustworthy (higher = worse)
    lic = licence_class(F.col("licence"))
    sev = F.upper(F.coalesce(F.col("vuln_max_severity"), F.lit("")))
    malicious = F.coalesce(F.col("malicious"), F.lit(False))
    vuln_high = ~malicious & sev.isin("HIGH", "CRITICAL")
    vuln_other = ~malicious & ~vuln_high & (sev != "")
    stale_days = F.datediff(today, F.col("last_updated"))
    stale = F.coalesce(F.col("archived"), F.lit(False)) | F.coalesce(F.col("deprecated"), F.lit(False)) \
        | F.coalesce(stale_days > 365, F.lit(False))
    no_provenance = (F.col("category").isin("mcp_server", "package") & ~F.coalesce(F.col("has_repo"), F.lit(False))) \
        | (F.col("category").isin("model", "dataset") & ~F.coalesce(F.col("publisher_trusted"), F.lit(False)))
    unsanctioned = ~F.coalesce(F.col("sanctioned"), F.lit(False))
    trusted = F.coalesce(F.col("publisher_trusted"), F.lit(False)) & (lic == "permissive") & ~stale
    gov_rules = [
        (malicious,               5.0, "known-malicious version"),
        (vuln_high,               4.0, "open high/critical advisory on the version in use"),
        (vuln_other,              2.5, "open advisory on the version in use"),
        (lic == "unknown",        2.0, "no or unclear licence"),
        (lic == "restricted",     1.0, "restrictive or non-commercial licence"),
        (no_provenance,           2.0, "unverified publisher or no linked source repo"),
        (stale,                   1.5, "archived, deprecated or not updated for 12+ months"),
        (unsanctioned,            2.0, "not on the company allow-list"),
        (trusted,                -2.0, "trusted publisher, permissive licence, actively maintained"),
    ]
    governance = F.lit(1.0)
    for cond, pts, label in gov_rules:
        governance = governance + F.when(cond, pts).otherwise(0.0)
        reasons.append(_reason(cond, pts, label))
    governance = F.greatest(F.lit(0.0), F.least(F.lit(10.0), governance))

    composite = F.round(WEIGHTS["capability"] * capability + WEIGHTS["exposure"] * exposure
                        + WEIGHTS["governance"] * governance, 1)
    floored = (F.when(malicious | vuln_high & (sev == "CRITICAL"), F.greatest(composite, F.lit(9.0)))
               .when(unsanctioned & (capability >= 7.0), F.greatest(composite, F.lit(7.0)))
               .when(unsanctioned & (governance >= 7.0) & (capability >= 4.0), F.greatest(composite, F.lit(5.0)))
               .otherwise(composite))

    ranked = F.transform(F.sort_array(F.filter(F.array(*reasons), lambda r: r.isNotNull()), asc=False),
                         lambda r: r["label"])
    out = (df.withColumn("capability", F.round(capability, 1).cast("decimal(3,1)"))
             .withColumn("exposure", F.round(exposure, 1).cast("decimal(3,1)"))
             .withColumn("governance", F.round(governance, 1).cast("decimal(3,1)"))
             .withColumn("risk_score", F.round(floored, 1).cast("decimal(3,1)"))
             .withColumn("score_reasons", ranked))
    return out.withColumn("band", band(F.col("risk_score")))


def band(score: Column) -> Column:
    return (F.when(score >= 9.0, "Critical").when(score >= 7.0, "High")
             .when(score >= 5.0, "Medium").otherwise("Low"))
