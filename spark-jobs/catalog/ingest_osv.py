"""Ingest OSV.dev advisories for the version in use of every known package into bronze.osv_advisories_raw.

Two record types share the table:
  source=osv_query  key='pypi:<name>@<version>'  -> list of vulnerability ids affecting that version
  source=osv_vuln   key='<vuln id>'              -> vulnerability details (severity, summary, aliases)
Vulnerability details are immutable enough to fetch once; queries are re-run every day because new
advisories (e.g. a poisoned release) appear without the package changing.
"""
import argparse

from pyspark.sql import functions as F

from common import http_json, latest_bronze, log, new_run_id, parallel_map, spark_session, write_bronze

BATCH = 500
ECOSYSTEM = {"pypi": "PyPI", "npm": "npm"}


def package_versions(spark) -> list[tuple[str, str, str]]:
    pkgs = latest_bronze(spark, "tracewell.bronze.pkg_metadata_raw").where(~F.col("payload").contains('"not_found":true'))
    rows = (pkgs.select("source", "record_key",
                        F.coalesce(F.get_json_object("payload", "$.info.version"),
                                   F.get_json_object("payload", "$.version")).alias("version"))
            .where("version IS NOT NULL").collect())
    return [(r.source, r.record_key, r.version) for r in rows]


def query_batch(chunk: list[tuple[str, str, str]]) -> list[list[str]]:
    body = {"queries": [{"package": {"name": name, "ecosystem": ECOSYSTEM[eco]}, "version": ver}
                        for eco, name, ver in chunk]}
    data, _ = http_json("https://api.osv.dev/v1/querybatch", data=body, timeout=120)
    return [[v["id"] for v in r.get("vulns", [])] for r in data["results"]]


def fetch_vuln(vid: str) -> dict:
    d, _ = http_json(f"https://api.osv.dev/v1/vulns/{vid}")
    return {
        "id": d.get("id"),
        "summary": d.get("summary"),
        "aliases": d.get("aliases"),
        "published": d.get("published"),
        "modified": d.get("modified"),
        "withdrawn": d.get("withdrawn"),
        "severity": (d.get("database_specific") or {}).get("severity"),
        "cvss": [s.get("score") for s in d.get("severity") or []],
        "malicious": d.get("id", "").startswith("MAL-"),
    }


def main() -> None:
    argparse.ArgumentParser().parse_args()
    spark = spark_session("ingest-osv")
    run_id = new_run_id()

    pkgs = package_versions(spark)
    log(f"querying OSV for {len(pkgs)} package versions")
    queries, all_ids = [], set()
    for i in range(0, len(pkgs), BATCH):
        chunk = pkgs[i:i + BATCH]
        for (eco, name, ver), ids in zip(chunk, query_batch(chunk)):
            queries.append((f"{eco}:{name}@{ver}", {"ecosystem": eco, "name": name, "version": ver, "vuln_ids": ids}))
            all_ids.update(ids)
    write_bronze(spark, "tracewell.bronze.osv_advisories_raw", run_id, "osv_query", queries)

    known = {r.record_key for r in spark.table("tracewell.bronze.osv_advisories_raw")
             .where("source = 'osv_vuln'").select("record_key").distinct().collect()}
    new_ids = sorted(all_ids - known)
    log(f"{len(all_ids)} vulnerabilities referenced, {len(new_ids)} new to fetch")
    details = parallel_map(fetch_vuln, new_ids, workers=12)
    vulns = [(vid, d) for vid, d in zip(new_ids, details) if not isinstance(d, Exception)]
    write_bronze(spark, "tracewell.bronze.osv_advisories_raw", run_id, "osv_vuln", vulns)
    affected = sum(1 for _, q in queries if q["vuln_ids"])
    log(f"done: {affected} of {len(queries)} package versions have open advisories")
    spark.stop()


if __name__ == "__main__":
    main()
