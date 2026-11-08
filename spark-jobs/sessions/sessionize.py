"""Batch sessionization with exactly the rules of the Flink session windows (flink-jobs/.../Sessions.java).

A session is the run of one person's events on one device with no pause of `gap` or more between two
consecutive events. Start and end are the first and last event; the id is
sha256(device_id|identity|start epoch millis)[:16], so a session computed here has the same key as the
one Flink emitted for the same events.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

BANDS = ["Low", "Medium", "High", "Critical"]


def millis(col: str) -> Column:
    return F.expr(f"unix_millis({col})")


def session_id(device: str = "device_id", identity: str = "identity", start: str = "session_start") -> Column:
    return F.substring(F.sha2(F.concat_ws("|", device, identity, millis(start).cast("string")), 256), 1, 16)


def _count_if(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0)).cast("long")


def sessionize(events: DataFrame, gap_ms: int, source: str = "backfill") -> DataFrame:
    """events: rows of silver.fct_ai_events. Returns rows shaped like silver.fct_sessions (without built_at)."""
    w = Window.partitionBy("device_id", "identity").orderBy("ts_event", "event_id")
    bands = ", ".join(f"'{b}'" for b in BANDS)
    band_rank = F.expr(f"array_position(array({bands}), band_at_event)")
    ev = (events.withColumn("_ms", millis("ts_event"))
          .withColumn("_new", F.when(F.col("_ms") - F.lag("_ms").over(w) >= gap_ms, 1).otherwise(0))
          .withColumn("_n", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow))))
    s = ev.groupBy("device_id", "identity", "_n").agg(
        F.first("department", ignorenulls=True).alias("department"),
        F.min("ts_event").alias("session_start"),
        F.max("ts_event").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("sensor")).alias("sensors_seen"),
        F.sort_array(F.collect_set("event_type")).alias("event_types"),
        F.sort_array(F.collect_set("asset_id")).alias("assets_used"),
        F.size(F.collect_set(F.when(~F.col("asset_known"), F.col("asset_id")))).alias("unknown_assets"),
        F.max("risk_score_at_event").alias("max_risk_score"),
        F.max(band_rank).alias("_band"),
        _count_if(F.col("sensitive_count") > 0).alias("sensitive_events"),
        _count_if(F.col("action") == "block").alias("blocks"),
        _count_if(F.col("action") == "warn").alias("warns"),
        F.round(F.coalesce(F.sum("bytes_out"), F.lit(0)) / 1e6, 2).alias("mb_out"))
    return s.select(
        session_id().alias("session_id"),
        "identity", "device_id", "department", "session_start", "session_end",
        F.floor((millis("session_end") - millis("session_start")) / 1000).cast("long").alias("duration_s"),
        "event_count", "sensors_seen", "event_types", "assets_used", F.col("unknown_assets").cast("int").alias("unknown_assets"),
        "max_risk_score",
        F.when(F.col("_band") > 0, F.element_at(F.array(*map(F.lit, BANDS)), F.col("_band").cast("int"))).alias("max_band"),
        "sensitive_events", "blocks", "warns", "mb_out", F.lit(source).alias("source"))
