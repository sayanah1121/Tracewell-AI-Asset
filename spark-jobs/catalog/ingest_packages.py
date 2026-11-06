"""Ingest PyPI and npm metadata into bronze.pkg_metadata_raw.

Which packages:
  * the curated seed list (AI SDKs, agent frameworks) and every npm/PyPI package a curated MCP server
    declares (seed/curated_assets.csv, column packages);
  * discovered npm packages: the top results of npm searches for AI keywords (mcp, llm, openai, ...),
    ranked by npm's own score (relevance + popularity). A search page already carries version, licence,
    repo, publish date and monthly downloads, so 250 packages cost one request;
  * discovered PyPI projects: names from the PyPI simple index that contain an AI token (llm, gpt, mcp,
    agent, ...), most recently active first (highest _last-serial), each fetched from the JSON API.
Packages fetched in the last --refresh-days are skipped, so daily runs only top up new or stale packages.
"""
import argparse
import csv
import os
import re
import urllib.parse

from pyspark.sql import functions as F

from common import HttpError, env_int, http_json, log, new_run_id, parallel_map, spark_session, write_bronze

SEED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seed")
NPM_QUERIES = ["keywords:mcp", "keywords:llm", "keywords:openai", "keywords:ai-agent", "keywords:langchain",
               "keywords:anthropic", "keywords:mcp-server", "keywords:chatgpt", "keywords:ollama", "keywords:rag",
               "keywords:embeddings", "keywords:generative-ai", "keywords:agent", "keywords:gpt", "keywords:claude",
               "keywords:gemini", "keywords:ai"]
AI_NAME = re.compile(r"(^|[-_.])(llm|llms|gpt|openai|anthropic|claude|langchain|llama|mcp|ollama|chatgpt|rag|agent|"
                     r"agents|genai|gemini|mistral|transformers?|embeddings?|copilot|autogen|crewai|vllm|lora|"
                     r"diffusion)\d*([-_.]|$)", re.I)
BATCH = 1000  # bronze commit every BATCH fetched packages, so a crash keeps what was fetched


def pypi_norm(name: str) -> str:
    """PEP 503 normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


def wanted_packages() -> list[tuple[str, str]]:
    with open(os.path.join(SEED_DIR, "seed_packages.csv"), newline="", encoding="utf-8") as f:
        seeds = [(r["ecosystem"], r["name"]) for r in csv.DictReader(f)]
    with open(os.path.join(SEED_DIR, "curated_assets.csv"), newline="", encoding="utf-8") as f:
        declared = [tuple(k.split(":", 1)) for r in csv.DictReader(f) for k in r["packages"].split(";") if k]
    out, seen = [], set()
    for eco, name in seeds + declared:
        key = (eco, pypi_norm(name) if eco == "pypi" else name.lower().strip())
        if key[1] and key not in seen:
            seen.add(key)
            out.append(key)
    log(f"packages wanted: {len(seeds)} seeds + {len(declared)} declared by curated MCP servers -> {len(out)} unique")
    return out


def is_ai_name(name: str) -> bool:
    """True when a package name contains an AI token as a whole word: 'llm-guard', 'mcp_server_x', 'agents'."""
    return bool(AI_NAME.search(name))


def pick_pypi(projects: list[dict], target: int) -> list[str]:
    """AI-named projects from the PyPI simple index, most recently active first (highest _last-serial)."""
    ai = [p for p in projects if is_ai_name(p["name"])]
    ai.sort(key=lambda p: (-p.get("_last-serial", 0), p["name"]))
    return [pypi_norm(p["name"]) for p in ai[:target]]


def discover_pypi(target: int) -> list[str]:
    if target <= 0:
        return []
    index, _ = http_json("https://pypi.org/simple/", headers={"Accept": "application/vnd.pypi.simple.v1+json"},
                         timeout=180)
    names = pick_pypi(index["projects"], target)
    log(f"pypi discovery: {len(index['projects'])} projects in the index -> {len(names)} AI-named, most active first")
    return names


def npm_record(hit: dict) -> dict:
    """An npm search hit in the same shape as fetch_npm(), so resolve_entities parses both alike."""
    pkg = hit["package"]
    links = pkg.get("links") or {}
    return {"name": pkg["name"], "version": pkg.get("version"), "description": pkg.get("description"),
            "license": pkg.get("license"),
            "repository": {"url": links["repository"]} if links.get("repository") else None,
            "homepage": links.get("homepage"), "keywords": pkg.get("keywords"), "deprecated": None,
            "maintainers": len(pkg.get("maintainers") or []), "date": pkg.get("date"),
            "downloads_monthly": (hit.get("downloads") or {}).get("monthly")}


def discover_npm(target: int, page_size: int = 250) -> dict[str, dict]:
    """Round-robin over the AI keyword searches, a page at a time, until `target` unique packages."""
    found: dict[str, dict] = {}
    offsets = {q: 0 for q in NPM_QUERIES}
    while offsets and len(found) < target:
        for q in list(offsets):
            params = urllib.parse.urlencode({"text": q, "size": page_size, "from": offsets[q]})
            try:
                page, _ = http_json(f"https://registry.npmjs.org/-/v1/search?{params}")
            except HttpError as e:
                log(f"npm search {q} at {offsets[q]} failed ({e.status}); skipping that query")
                del offsets[q]
                continue
            hits = page.get("objects") or []
            for hit in hits:
                found.setdefault(hit["package"]["name"].lower().strip(), npm_record(hit))
            offsets[q] += page_size
            if len(hits) < page_size:
                del offsets[q]
            if len(found) >= target:
                break
    log(f"npm discovery: {len(found)} unique packages from {len(NPM_QUERIES)} keyword searches")
    return dict(list(found.items())[:target])


def fetch_pypi(name: str) -> dict | None:
    try:
        data, _ = http_json(f"https://pypi.org/pypi/{urllib.parse.quote(name)}/json")
    except HttpError as e:
        if e.status == 404:
            return None
        raise
    info = {k: v for k, v in data["info"].items() if k != "description"}  # drop README body
    uploads = [u.get("upload_time_iso_8601") for u in data.get("urls", []) if u.get("upload_time_iso_8601")]
    return {"info": info, "latest_upload": max(uploads) if uploads else None,
            "vulnerabilities": [{"id": v.get("id"), "aliases": v.get("aliases")} for v in data.get("vulnerabilities", [])]}


def fetch_npm(name: str) -> dict | None:
    quoted = urllib.parse.quote(name, safe="@")
    try:
        latest, _ = http_json(f"https://registry.npmjs.org/{quoted.replace('/', '%2f')}/latest")
    except HttpError as e:
        if e.status == 404:
            return None
        raise
    keep = ("name", "version", "description", "license", "repository", "homepage", "keywords",
            "deprecated", "author", "bin")
    out = {k: latest.get(k) for k in keep}
    out["maintainers"] = len(latest.get("maintainers") or [])
    # The search API carries the publish date and monthly downloads in one call.
    try:
        search, _ = http_json(f"https://registry.npmjs.org/-/v1/search?{urllib.parse.urlencode({'text': name, 'size': 5})}")
        hit = next((o for o in search.get("objects", []) if o["package"]["name"] == name), None)
        if hit:
            out["date"] = hit["package"].get("date")
            out["downloads_monthly"] = (hit.get("downloads") or {}).get("monthly")
    except HttpError:
        pass
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-packages", type=int, default=env_int("CATALOG_MAX_PACKAGES", 25000),
                    help="cap on packages fetched one by one per run")
    ap.add_argument("--npm-discover", type=int, default=env_int("CATALOG_NPM_DISCOVER", 12000))
    ap.add_argument("--pypi-discover", type=int, default=env_int("CATALOG_PYPI_DISCOVER", 8000))
    ap.add_argument("--refresh-days", type=int, default=7)
    args = ap.parse_args()

    spark = spark_session("ingest-packages")
    run_id = new_run_id()
    table = "tracewell.bronze.pkg_metadata_raw"
    fresh = {(r.source, r.record_key) for r in
             spark.table(table)
             .where(F.col("fetched_at") > F.expr(f"current_timestamp() - INTERVAL {args.refresh_days} DAYS"))
             .select("source", "record_key").distinct().collect()}

    # npm discovery: the search pages are the metadata, so they go straight to bronze.
    npm_found = discover_npm(args.npm_discover) if args.npm_discover > 0 else {}
    npm_new = [(name, rec) for name, rec in npm_found.items() if ("npm", name) not in fresh]
    write_bronze(spark, table, run_id, "npm", npm_new)
    fresh |= {("npm", name) for name in npm_found}

    # Seeds and declared packages first, so the cap never drops them; then discovered PyPI projects.
    wanted, seen = [], set()
    for p in wanted_packages() + [("pypi", n) for n in discover_pypi(args.pypi_discover)]:
        if p not in seen and p not in fresh:
            seen.add(p)
            wanted.append(p)
    todo = wanted[: args.max_packages]
    log(f"{len(fresh)} fetched recently or found by npm search; fetching {len(todo)} one by one")

    fetchers = {"pypi": fetch_pypi, "npm": fetch_npm}
    written = errors = missing = 0
    for start in range(0, len(todo), BATCH):
        chunk = todo[start:start + BATCH]
        results = parallel_map(lambda p: fetchers[p[0]](p[1]), chunk, workers=16)
        by_eco: dict[str, list] = {"pypi": [], "npm": []}
        for (eco, name), res in zip(chunk, results):
            if isinstance(res, Exception):
                errors += 1
            elif res is None:
                missing += 1
                by_eco[eco].append((name, {"not_found": True}))
            else:
                by_eco[eco].append((name, res))
        for eco, records in by_eco.items():
            write_bronze(spark, table, run_id, eco, records)
            written += len(records)
        log(f"fetched {start + len(chunk)}/{len(todo)}")
    log(f"done: {len(npm_new)} npm search records + {written} fetched written, {missing} not found, {errors} errors")
    spark.stop()


if __name__ == "__main__":
    main()
