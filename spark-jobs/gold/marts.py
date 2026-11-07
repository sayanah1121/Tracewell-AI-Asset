"""Gold marts: the business tables behind the dashboards, built from silver by Spark.

Every mart is a pure function of DataFrames, so it can be unit-tested (tests/gold) without a cluster.
build_gold.py reads the inputs, limits them to the days being rebuilt and overwrites only those
partitions, which makes every run idempotent (rerun an hour, get the same rows).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

BANDS = ["Low", "Medium", "High", "Critical"]
LATE_AFTER_S = 300  # 5 min: beyond the Flink job's out-of-orderness bound, so the event arrived "late"

# Score reasons (silver.dim_asset_risk.score_reasons) -> framework controls. A best-effort learning aid,
# not legal advice; see docs/05-risk-scoring.md section 6.
CROSSWALK = [
    # (text in the score reason,              OWASP LLM Top 10 (2025),                    NIST AI RMF, EU AI Act theme)
    ("can execute code or shell commands",    "LLM06 Excessive Agency",                    "MANAGE",  "Human oversight"),
    ("can write or delete data",              "LLM06 Excessive Agency",                    "MANAGE",  "Human oversight"),
    ("acts autonomously",                     "LLM06 Excessive Agency",                    "MANAGE",  "Human oversight"),
    ("handles credentials or payments",       "LLM02 Sensitive Information Disclosure",    "MEASURE", "Data governance"),
    ("hosted remotely by a third party",      "LLM02 Sensitive Information Disclosure",    "MAP",     "Data governance"),
    ("has outbound network access",           "LLM01 Prompt Injection",                    "MEASURE", "Robustness & cybersecurity"),
    ("known-malicious version",               "LLM03 Supply Chain",                        "MAP",     "Robustness & cybersecurity"),
    ("advisory on the version in use",        "LLM03 Supply Chain",                        "MAP",     "Robustness & cybersecurity"),
    ("unverified publisher",                  "LLM03 Supply Chain",                        "GOVERN",  "Transparency"),
    ("no or unclear licence",                 "LLM03 Supply Chain",                        "GOVERN",  "Transparency"),
    ("restrictive or non-commercial licence", "LLM03 Supply Chain",                        "GOVERN",  "Transparency"),
    ("archived, deprecated",                  "LLM03 Supply Chain",                        "GOVERN",  "Transparency"),
    ("not on the company allow-list",         None,                                        "GOVERN",  "Human oversight"),
]
SENSITIVE_CONTROL = ("LLM02 Sensitive Information Disclosure", "MEASURE", "Data governance")


def band_rank(band: Column) -> Column:
    rank = F.lit(None).cast("int")
    for i, b in enumerate(BANDS, start=1):
        rank = F.when(band == b, F.lit(i)).otherwise(rank)
    return rank


def band_name(rank: Column) -> Column:
    name = F.lit(None).cast("string")
    for i, b in enumerate(BANDS, start=1):
        name = F.when(rank == i, F.lit(b)).otherwise(name)
    return name


def _count_if(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0)).cast("long")


def _pct(part: Column, whole: Column) -> Column:
    return F.when(whole > 0, F.round(100.0 * part / whole, 2)).otherwise(F.lit(0.0))


def _verdict_counts() -> list[Column]:
    return [_count_if(F.col("sensitive_count") > 0).alias("sensitive_events"),
            _count_if(F.col("action") == "block").alias("blocks"),
            _count_if(F.col("action") == "warn").alias("warns")]


# ───────────────────────────── shadow AI ─────────────────────────────

def shadow_ai(events: DataFrame, current: DataFrame, first_seen: DataFrame) -> DataFrame:
    """asset × day: AI in use that the company has not sanctioned, or that the catalog has never seen.

    events: silver.fct_ai_events rows for the days being built. current: is_current rows of dim_asset_risk.
    first_seen: (asset_id, first_seen) over all history, so "new today" is exact.
    `seen_only_on_device` = no network-sensor event that day: a proxy/CASB-only tool would have missed it.
    """
    shadow = events.where(~F.col("asset_known") | ~F.coalesce(F.col("sanctioned_at_event"), F.lit(False)))
    daily = (shadow.groupBy(F.to_date("ts_event").alias("day"), "asset_id")
             .agg(F.max("asset_known").alias("asset_known"),
                  F.first("asset_category", ignorenulls=True).alias("category"),
                  F.min("ts_event").alias("first_seen_day"),
                  F.max("ts_event").alias("last_seen_day"),
                  F.count("*").alias("events"),
                  F.countDistinct("device_id").alias("devices"),
                  F.countDistinct("identity").alias("identities"),
                  F.sort_array(F.collect_set("department")).alias("departments"),
                  *_verdict_counts(),
                  F.round(F.coalesce(F.sum("bytes_out"), F.lit(0)) / 1e6, 2).alias("mb_out"),
                  F.sort_array(F.collect_set("file_name")).alias("files_touched"),
                  F.max(F.col("sensor") == "network").alias("seen_by_network"),
                  band_name(F.max(band_rank(F.col("band_at_event")))).alias("max_band_at_event")))
    cur = current.select("asset_id", "asset_name", F.col("band").alias("current_band"),
                         F.col("risk_score").cast("double").alias("current_risk_score"),
                         F.col("sanctioned").alias("sanctioned_now"))
    return (daily.join(cur, "asset_id", "left").join(first_seen, "asset_id", "left")
            .select("day", "asset_id",
                    F.coalesce("asset_name", "asset_id").alias("asset_name"),
                    F.when(F.col("asset_known"), F.col("category")).otherwise(F.lit("unknown")).alias("category"),
                    "asset_known",
                    F.coalesce("current_band", "max_band_at_event").alias("current_band"),
                    "current_risk_score", "max_band_at_event",
                    F.coalesce("sanctioned_now", F.lit(False)).alias("sanctioned_now"),
                    F.coalesce("first_seen", "first_seen_day").alias("first_seen"),
                    (F.to_date(F.coalesce("first_seen", "first_seen_day")) == F.col("day")).alias("is_new"),
                    "last_seen_day", "events", "devices", "identities", "departments",
                    "sensitive_events", "blocks", "warns", "mb_out", "files_touched",
                    (~F.col("seen_by_network")).alias("seen_only_on_device")))


def first_seen_per_asset(all_events: DataFrame) -> DataFrame:
    return all_events.groupBy("asset_id").agg(F.min("ts_event").alias("first_seen"))


# ───────────────────────────── fleet risk posture ─────────────────────────────

def fleet_risk_posture(events: DataFrame) -> DataFrame:
    """department × day: how risky the AI that each department actually uses is, and what the policies did."""
    ev = events.withColumn("day", F.to_date("ts_event")).withColumn(
        "band_bucket", F.when(F.col("asset_known"), F.col("band_at_event")).otherwise(F.lit("Unknown")))
    buckets = BANDS + ["Unknown"]
    per_event = ev.groupBy("day", "department").agg(
        F.count("*").alias("events"),
        F.countDistinct("identity").alias("active_people"),
        F.countDistinct("device_id").alias("active_devices"),
        _count_if(F.col("band_bucket").isin("High", "Critical", "Unknown")).alias("high_risk_events"),
        _count_if(~F.coalesce(F.col("sanctioned_at_event"), F.lit(False))).alias("unsanctioned_events"),
        *_verdict_counts())
    # Asset level first, so the averages weigh each asset once rather than each event.
    per_asset = (ev.groupBy("day", "department", "asset_id")
                 .agg(F.max(F.when(F.col("band_bucket") == "Unknown", 5).otherwise(band_rank(F.col("band_bucket"))))
                      .alias("_rank"), F.max("risk_score_at_event").alias("tracewell"))
                 .withColumn("band_bucket", F.when(F.col("_rank") == 5, "Unknown").otherwise(band_name(F.col("_rank"))))
                 .groupBy("day", "department")
                 .agg(F.countDistinct("asset_id").alias("distinct_assets"),
                      *[_count_if(F.col("band_bucket") == b).alias(f"assets_{b.lower()}") for b in buckets],
                      F.round(F.avg("tracewell"), 2).alias("mean_risk_score"),
                      F.max("tracewell").alias("max_risk_score")))
    return (per_event.join(per_asset, ["day", "department"])
            .withColumn("high_risk_event_pct", _pct(F.col("high_risk_events"), F.col("events")))
            .select("day", "department", "events", "active_people", "active_devices", "distinct_assets",
                    "assets_critical", "assets_high", "assets_medium", "assets_low", "assets_unknown",
                    "mean_risk_score", "max_risk_score", "high_risk_events", "high_risk_event_pct",
                    "unsanctioned_events", "sensitive_events", "blocks", "warns"))


# ───────────────────────────── policy hits ─────────────────────────────

def policy_hits(events: DataFrame, current: DataFrame, policies: DataFrame) -> DataFrame:
    """day × policy × department × asset: every block and warn the stream issued, with what triggered it.

    policies: (policy_id, policy_name) from policies/*.yaml, the same rules Flink holds in broadcast state.
    """
    hits = (events.where(F.col("action") != "allow")
            .groupBy(F.to_date("ts_event").alias("day"), "policy_id", "action", "department", "asset_id")
            .agg(F.count("*").alias("hits"),
                 F.countDistinct("device_id").alias("devices"),
                 F.countDistinct("identity").alias("people"),
                 F.min("ts_event").alias("first_hit"),
                 F.max("ts_event").alias("last_hit"),
                 F.sort_array(F.collect_set("event_type")).alias("event_types"),
                 F.sort_array(F.collect_set("file_name")).alias("files"),
                 F.sort_array(F.array_distinct(F.flatten(F.collect_list(
                     F.coalesce("sensitive_types", F.array().cast("array<string>")))))).alias("sensitive_types"),
                 F.round(F.coalesce(F.sum("bytes_out"), F.lit(0)) / 1e6, 2).alias("mb_out")))
    return (hits.join(policies, "policy_id", "left").join(current.select("asset_id", "asset_name"), "asset_id", "left")
            .select("day", "policy_id", F.coalesce("policy_name", "policy_id").alias("policy_name"), "action",
                    "department", "asset_id", F.coalesce("asset_name", "asset_id").alias("asset_name"),
                    "hits", "devices", "people", "first_hit", "last_hit", "event_types", "files",
                    "sensitive_types", "mb_out"))


# ───────────────────────────── AI ROI ─────────────────────────────

def ai_roi(events: DataFrame, licences: DataFrame, people: DataFrame, months: DataFrame) -> DataFrame:
    """tool × department × month: paid seats vs people who actually used the tool.

    Active users are counted against the HR directory (people), so only current employees fill a seat.
    `overlap` = the department pays for two or more tools that do the same job.
    """
    active = (events.join(people.select("identity", F.col("department").alias("home_department")), "identity")
              .groupBy(F.trunc("ts_event", "month").alias("month"), F.col("asset_id").alias("tool_id"),
                       F.col("home_department").alias("department"))
              .agg(F.countDistinct("identity").alias("active_users"), F.count("*").alias("events")))
    same_job = (licences.groupBy("department", "function")
                .agg(F.sort_array(F.collect_set("tool_name")).alias("overlapping_tools")))
    return (licences.crossJoin(months)
            .join(active, ["month", "tool_id", "department"], "left")
            .join(same_job, ["department", "function"])
            .select("month", "tool_id", "tool_name", "function", "department", "seats",
                    F.col("cost_per_seat_month").cast("double").alias("cost_per_seat_month"),
                    F.coalesce("active_users", F.lit(0)).cast("long").alias("active_users"),
                    F.coalesce("events", F.lit(0)).cast("long").alias("events"),
                    "overlapping_tools")
            .withColumn("utilisation_pct", _pct(F.col("active_users"), F.col("seats")))
            .withColumn("idle_seats", F.greatest(F.col("seats") - F.col("active_users"), F.lit(0)).cast("long"))
            .withColumn("users_without_seat", F.greatest(F.col("active_users") - F.col("seats"), F.lit(0)).cast("long"))
            .withColumn("monthly_cost", F.col("seats") * F.col("cost_per_seat_month"))
            .withColumn("idle_cost", F.col("idle_seats") * F.col("cost_per_seat_month"))
            .withColumn("overlap", F.size("overlapping_tools") > 1))


# ───────────────────────────── compliance evidence ─────────────────────────────

def compliance_evidence(current: DataFrame, recent_events: DataFrame) -> DataFrame:
    """asset (current version): the risk dimensions, the reasons, and the framework controls they map to.

    recent_events: the last 30 days of silver.fct_ai_events; sensitive data seen in them adds LLM02 evidence.
    """
    usage = recent_events.where(F.col("asset_known")).groupBy("asset_id").agg(
        F.countDistinct("device_id").alias("devices_30d"),
        F.count("*").alias("events_30d"),
        _count_if(F.col("sensitive_count") > 0).alias("sensitive_events_30d"),
        F.max("ts_event").alias("last_seen"))
    df = current.join(usage, "asset_id", "left").fillna(0, ["devices_30d", "events_30d", "sensitive_events_30d"])

    def mentions(text: str) -> Column:  # a factory: Spark reads the lambda's arity, so no default arguments
        return F.exists("score_reasons", lambda r: r.contains(text))

    hits = []
    for text, owasp, nist, eu in CROSSWALK:
        cond = mentions(text)
        hits.append(F.when(cond, F.struct(F.lit(owasp).alias("owasp"), F.lit(nist).alias("nist"),
                                          F.lit(eu).alias("eu"), F.lit(text).alias("signal"))))
    owasp, nist, eu = SENSITIVE_CONTROL
    hits.append(F.when(F.col("sensitive_events_30d") > 0, F.struct(
        F.lit(owasp).alias("owasp"), F.lit(nist).alias("nist"), F.lit(eu).alias("eu"),
        F.format_string("sensitive data in %d events (last 30 days)", F.col("sensitive_events_30d")).alias("signal"))))
    df = df.withColumn("_hits", F.filter(F.array(*hits), lambda h: h.isNotNull()))

    def distinct_of(field: str) -> Column:
        return F.array_sort(F.array_distinct(F.filter(F.transform("_hits", lambda h: h[field]), lambda v: v.isNotNull())))

    return df.select(
        "asset_id", "asset_name", "category", "publisher", "licence", "band",
        *[F.col(c).cast("double").alias(c) for c in ("risk_score", "capability", "exposure", "governance")],
        "sanctioned", "score_reasons",
        distinct_of("owasp").alias("owasp_llm"), distinct_of("nist").alias("nist_ai_rmf"),
        distinct_of("eu").alias("eu_ai_act"),
        F.transform("_hits", lambda h: F.concat_ws(" | ", F.coalesce(h["owasp"], F.lit("-")), h["nist"], h["eu"],
                                                   h["signal"])).alias("evidence"),
        (F.col("devices_30d") > 0).alias("in_use_30d"),
        F.col("devices_30d").cast("long").alias("devices_30d"),
        F.col("events_30d").cast("long").alias("events_30d"),
        F.col("sensitive_events_30d").cast("long").alias("sensitive_events_30d"),
        "last_seen", F.col("valid_from").alias("profile_valid_from"))


# ───────────────────────────── pipeline health ─────────────────────────────

def pipeline_health(bronze: DataFrame, silver: DataFrame, dlq: DataFrame, until=None) -> DataFrame:
    """hour: is the real-time path healthy? Volumes, resends, lateness, unknown assets, DLQ, verdict latency.

    bronze/silver are bucketed by when they were written (ingested_at / processed_at), dlq by Kafka timestamp.
    Every hour from the first one with data up to `until` (default: the last one with data) gets a row, so an
    hour in which nothing flowed shows as 0 events (latency null) instead of being silently skipped.
    """
    b = bronze.groupBy(F.date_trunc("hour", "ingested_at").alias("hour")).agg(
        F.count("*").alias("records_in"), F.countDistinct("event_id").alias("distinct_in"))
    late = (F.unix_timestamp("processed_at") - F.unix_timestamp("ts_event")) > LATE_AFTER_S
    s = silver.groupBy(F.date_trunc("hour", "processed_at").alias("hour")).agg(
        F.count("*").alias("events_out"),
        _count_if(late).alias("late_events"),
        _count_if(~F.col("asset_known")).alias("unknown_asset_events"),
        *_verdict_counts(),
        F.percentile_approx("latency_ms", [0.5, 0.95, 0.99], 1000).alias("_p"),
        F.max("latency_ms").alias("latency_max_ms"))
    d = dlq.groupBy(F.date_trunc("hour", "timestamp").alias("hour")).agg(F.count("*").alias("dlq_records"))
    out = b.join(s, "hour", "full").join(d, "hour", "full")
    last = F.max("hour") if until is None else F.greatest(F.max("hour"), F.lit(until).cast("timestamp"))
    hours = out.agg(F.explode(F.sequence(F.min("hour"), last, F.expr("INTERVAL 1 HOUR"))).alias("hour"))
    out = hours.join(out, "hour", "left").fillna(
        0, ["records_in", "distinct_in", "events_out", "late_events", "unknown_asset_events",
            "sensitive_events", "blocks", "warns", "dlq_records"])
    return out.select(
        F.to_date("hour").alias("day"), "hour", "records_in",
        (F.col("records_in") - F.col("distinct_in")).alias("resends_dropped"),
        "events_out", "late_events", _pct(F.col("late_events"), F.col("events_out")).alias("late_pct"),
        "unknown_asset_events", _pct(F.col("unknown_asset_events"), F.col("events_out")).alias("unknown_asset_pct"),
        "dlq_records", _pct(F.col("dlq_records"), F.col("records_in") + F.col("dlq_records")).alias("dlq_pct"),
        "sensitive_events", "blocks", "warns",
        F.col("_p")[0].alias("latency_p50_ms"), F.col("_p")[1].alias("latency_p95_ms"),
        F.col("_p")[2].alias("latency_p99_ms"), "latency_max_ms")


# ───────────────────────────── OCSF export ─────────────────────────────
# OCSF 1.3 Detection Finding (class 2004, category 2 "Findings"), the schema SIEMs and Amazon Security Lake
# ingest. One finding per warn/block verdict; the full event is in `ocsf` (JSON), key fields are columns too.
OCSF_VERSION = "1.3.0"
_SEVERITY = {1: "Informational", 2: "Low", 3: "Medium", 4: "High", 5: "Critical"}
_RISK_LEVEL = {"Low": 1, "Medium": 2, "High": 3, "Critical": 4}


def _enum(mapping: dict, key: Column) -> Column:
    return F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))])[key]


def ocsf_findings(events: DataFrame, policies: DataFrame) -> DataFrame:
    """warn/block verdict -> OCSF Detection Finding. Severity: block of a Critical asset 5, other block 4,
    warn with sensitive data 3, other warn 2. action_id follows OCSF: 1 Allowed (warn), 2 Denied (block)."""
    v = events.where(F.col("action") != "allow").join(policies, "policy_id", "left")
    blocked = F.col("action") == "block"
    severity_id = (F.when(blocked & (F.col("band_at_event") == "Critical"), 5).when(blocked, 4)
                   .when(F.coalesce(F.col("sensitive_count"), F.lit(0)) > 0, 3).otherwise(2))
    title = F.coalesce("policy_name", "policy_id")
    ocsf = F.struct(
        F.lit(2004).alias("class_uid"), F.lit("Detection Finding").alias("class_name"),
        F.lit(2).alias("category_uid"), F.lit("Findings").alias("category_name"),
        F.lit(1).alias("activity_id"), F.lit("Create").alias("activity_name"), F.lit(200401).alias("type_uid"),
        F.unix_millis("ts_event").alias("time"),
        severity_id.alias("severity_id"), _enum(_SEVERITY, severity_id).alias("severity"),
        F.lit(1).alias("status_id"), F.lit("New").alias("status"),
        F.when(blocked, 2).otherwise(1).alias("action_id"), F.when(blocked, "Denied").otherwise("Allowed").alias("action"),
        F.struct(F.col("event_id").alias("uid"), title.alias("title"),
                 F.concat_ws(" ", F.initcap("action"), F.lit("by"), F.col("policy_id"), F.lit("on"), F.col("asset_id"))
                  .alias("desc"),
                 F.array(F.lit("AI usage policy")).alias("types"),
                 F.struct(F.col("policy_id").alias("uid"), title.alias("name"), F.lit(1).alias("type_id"),
                          F.lit("Rule").alias("type")).alias("analytic")).alias("finding_info"),
        F.struct(F.col("device_id").alias("uid"), F.col("device_id").alias("name"),
                 F.struct(F.col("sensor").alias("name")).alias("agent")).alias("device"),
        F.struct(F.struct(F.col("identity").alias("uid"), F.col("identity").alias("email_addr"),
                          F.struct(F.col("department").alias("ou_name")).alias("org")).alias("user")).alias("actor"),
        F.array(F.struct(F.col("asset_id").alias("uid"), F.col("asset_id").alias("name"),
                         F.coalesce("asset_category", F.lit("unknown")).alias("type"),
                         F.array(F.coalesce("band_at_event", F.lit("Unknown"))).alias("labels"))).alias("resources"),
        _enum(_RISK_LEVEL, F.col("band_at_event")).alias("risk_level_id"),
        F.col("band_at_event").alias("risk_level"),
        F.round(F.col("risk_score_at_event") * 10).cast("int").alias("risk_score"),
        F.struct(F.lit(OCSF_VERSION).alias("version"), F.col("event_id").alias("uid"),
                 F.struct(F.lit("Tracewell").alias("name"), F.lit("Tracewell").alias("vendor_name"))
                  .alias("product")).alias("metadata"),
        F.struct(F.coalesce("sensitive_types", F.array().cast("array<string>")).alias("sensitive_types"),
                 F.col("event_type").alias("event_type")).alias("unmapped"),
    )
    return v.select(F.to_date("ts_event").alias("day"), F.col("ts_event").alias("time"), "event_id",
                    severity_id.alias("severity_id"), _enum(_SEVERITY, severity_id).alias("severity"), "action",
                    "policy_id", "device_id", "identity", "asset_id", F.to_json(ocsf).alias("ocsf"))
