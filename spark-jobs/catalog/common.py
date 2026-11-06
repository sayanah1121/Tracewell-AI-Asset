"""Shared helpers for the risk-catalog Spark jobs: Spark session, HTTP, bronze writes, pipeline state."""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

USER_AGENT = "tracewell-risk-catalog/0.1 (learning project)"

BRONZE_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("record_key", StringType(), False),
    StructField("payload", StringType(), False),
    StructField("fetched_at", TimestampType(), False),
])


def log(msg: str) -> None:
    print(f"[catalog] {datetime.now(timezone.utc):%H:%M:%S} {msg}", flush=True)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def spark_session(app: str) -> SparkSession:
    spark = SparkSession.builder.appName(f"tracewell-catalog-{app}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ───────────────────────────── HTTP ─────────────────────────────

class HttpError(RuntimeError):
    def __init__(self, status: int, url: str, body: str = ""):
        super().__init__(f"HTTP {status} for {url}: {body[:200]}")
        self.status = status


def http_json(url: str, *, data: Any = None, headers: dict | None = None,
              retries: int = 4, timeout: int = 60) -> tuple[Any, dict]:
    """GET (or POST when data is given) a JSON endpoint. Retries 429/5xx with backoff.

    Returns (parsed_json, response_headers). 404 raises HttpError(404) without retrying.
    """
    body = json.dumps(data).encode() if data is not None else None
    hdrs = {"User-Agent": USER_AGENT, "Accept": "application/json", **(headers or {})}
    if body is not None:
        hdrs["Content-Type"] = "application/json"
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=body, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8")), dict(resp.headers)
        except urllib.error.HTTPError as e:
            text = e.read().decode("utf-8", "replace")
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(min(60, 2 ** attempt * 2))
                continue
            raise HttpError(e.code, url, text) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt < retries:
                time.sleep(min(60, 2 ** attempt * 2))
                continue
            raise
    raise AssertionError("unreachable")


def parallel_map(fn: Callable, items: Iterable, workers: int = 12) -> list:
    """Run fn over items with a thread pool; exceptions are returned as values, not raised."""
    def safe(item):
        try:
            return fn(item)
        except Exception as e:  # noqa: BLE001 - one bad record must not fail the whole ingest
            return e
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(safe, items))


# ───────────────────────────── bronze ─────────────────────────────

def new_run_id() -> str:
    return f"{utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def write_bronze(spark: SparkSession, table: str, run_id: str, source: str,
                 records: list[tuple[str, Any]]) -> int:
    """Append raw records [(record_key, payload_obj), ...] to a bronze table."""
    if not records:
        log(f"{table}: nothing to write for source={source}")
        return 0
    now = utcnow()
    rows = [(run_id, source, key, json.dumps(payload, ensure_ascii=False, separators=(",", ":")), now)
            for key, payload in records]
    df = spark.createDataFrame(rows, BRONZE_SCHEMA).withColumn("ingested_at", F.current_timestamp())
    df.writeTo(table).append()
    log(f"{table}: appended {len(rows)} rows (source={source}, run={run_id})")
    return len(rows)


def latest_bronze(spark: SparkSession, table: str, source: str | None = None) -> DataFrame:
    """Latest version of every record_key (bronze is append-only, so dedupe on read)."""
    df = spark.table(table)
    if source:
        df = df.where(F.col("source") == source)
    w = Window.partitionBy("source", "record_key").orderBy(F.col("fetched_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ───────────────────────────── pipeline state ─────────────────────────────

STATE_TABLE = "tracewell.ops.pipeline_state"


def get_state(spark: SparkSession, component: str, key: str) -> str | None:
    rows = (spark.table(STATE_TABLE)
            .where((F.col("component") == component) & (F.col("key") == key))
            .select("value").collect())
    return rows[0].value if rows else None


def set_state(spark: SparkSession, component: str, key: str, value: str | None) -> None:
    spark.createDataFrame([(component, key, value)], "component string, key string, value string") \
        .createOrReplaceTempView("_state_update")
    spark.sql(f"""
        MERGE INTO {STATE_TABLE} t
        USING _state_update s
        ON t.component = s.component AND t.key = s.key
        WHEN MATCHED THEN UPDATE SET t.value = s.value, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (component, key, value, updated_at)
             VALUES (s.component, s.key, s.value, current_timestamp())
    """)


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    return int(value) if value else default
