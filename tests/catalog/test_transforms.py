from datetime import datetime

import pytest
from pyspark.sql import functions as F

from transforms import github_repo, licence_class, norm_name, pypi_key, score_assets


def one(spark, expr, value):
    return spark.createDataFrame([(value,)], "v string").select(expr(F.col("v")).alias("r")).first().r


@pytest.mark.parametrize("url,expected", [
    ("git+https://github.com/Owner/Repo.git", "owner/repo"),
    ("https://github.com/modelcontextprotocol/servers/tree/main/src/filesystem", "modelcontextprotocol/servers"),
    ("git@github.com:acme/tool.git#main", "acme/tool"),
    ("https://gitlab.com/acme/tool", None),
    (None, None),
])
def test_github_repo(spark, url, expected):
    assert one(spark, github_repo, url) == expected


@pytest.mark.parametrize("name,expected", [
    ("@modelcontextprotocol/server-postgres", "postgres"),
    ("io.github.acme/postgres-mcp-server", "postgres"),
    ("mcp-server-postgres", "postgres"),
    ("Brave_Search-MCP", "bravesearch"),
    ("mcp-server-", None),
])
def test_norm_name(spark, name, expected):
    assert one(spark, norm_name, name) == expected


def test_pypi_key_is_pep503(spark):
    assert one(spark, pypi_key, "Llama_Index.Core") == "pypi:llama-index-core"


@pytest.mark.parametrize("lic,expected", [
    ("MIT", "permissive"), ("apache-2.0", "permissive"), ("Apache 2.0", "permissive"),
    ("cc-by-nc-4.0", "restricted"), ("gpl-3.0", "restricted"), ("llama3.1", "permissive"),
    (None, "unknown"), ("other", "unknown"), ("SEE LICENSE IN LICENSE", "unknown"),
])
def test_licence_class(spark, lic, expected):
    assert one(spark, licence_class, lic) == expected


ROW_SCHEMA = ("category string, capability_text string, capability_tags array<string>, pipeline_tag string, "
              "pop_pct double, popularity_hint double, is_remote boolean, licence string, has_repo boolean, "
              "publisher_trusted boolean, last_updated timestamp, archived boolean, deprecated boolean, "
              "vuln_max_severity string, malicious boolean, sanctioned boolean")
RECENT = datetime(2026, 10, 1)


def scored(spark, **kw):
    row = dict(category="mcp_server", capability_text="", capability_tags=None, pipeline_tag=None, pop_pct=None,
               popularity_hint=None, is_remote=False, licence="MIT", has_repo=True, publisher_trusted=False,
               last_updated=RECENT, archived=False, deprecated=False, vuln_max_severity=None, malicious=False,
               sanctioned=False)
    row.update(kw)
    df = spark.createDataFrame([tuple(row[c.split()[0]] for c in ROW_SCHEMA.split(", "))], ROW_SCHEMA)
    return score_assets(df, today=F.lit(datetime(2026, 10, 25))).first()


def test_rogue_mcp_server_scores_high(spark):
    r = scored(spark, capability_text="execute shell commands and fetch any url", has_repo=False, licence=None)
    assert float(r.capability) == 9.0           # base 3 + shell 4 + network 2
    assert float(r.governance) == 7.0           # base 1 + no licence 2 + no repo 2 + unsanctioned 2
    assert r.band == "High"                     # unsanctioned + capability >= 7 floors the score at 7.0
    assert r.score_reasons[0].startswith("can execute code")


def test_critical_advisory_floors_to_critical(spark):
    r = scored(spark, capability_text="query a database", vuln_max_severity="CRITICAL")
    assert float(r.risk_score) == 9.0 and r.band == "Critical"


def test_malicious_package_is_critical(spark):
    r = scored(spark, category="package", malicious=True)
    assert r.band == "Critical"
    assert "known-malicious version (+5)" in r.score_reasons


def test_trusted_sanctioned_model_is_low(spark):
    r = scored(spark, category="model", pipeline_tag="feature-extraction", licence="apache-2.0",
               publisher_trusted=True, sanctioned=True, pop_pct=0.5)
    assert float(r.governance) == 0.0           # base 1 - trusted 2, clamped at 0
    assert r.band == "Low"


def test_capability_tags_from_seed(spark):
    r = scored(spark, category="agent", capability_tags=["code_exec", "file_write"], popularity_hint=4.5)
    assert float(r.capability) == 9.5           # base 3 + 4 + 2.5
    assert float(r.exposure) == 4.5
    assert any(s.startswith("widely used") for s in r.score_reasons)


def test_unvetted_mcp_server_is_at_least_medium(spark):
    r = scored(spark, capability_text="autonomous agent workflows", has_repo=False, licence=None)
    assert float(r.capability) == 4.5 and float(r.governance) == 7.0
    assert r.band == "Medium"                   # 0.35*4.5 + 0.40*7 = 4.4, floored to 5.0


def test_stale_and_remote(spark):
    r = scored(spark, last_updated=datetime(2024, 1, 1), is_remote=True)
    assert "archived, deprecated or not updated for 12+ months (+1.5)" in r.score_reasons
    assert float(r.exposure) == 1.5
