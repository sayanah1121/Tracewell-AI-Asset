-- Phase 2 streaming tables (Trino DDL: Flink SQL cannot declare hidden partition transforms).
-- Run with: ./tracewell.sh init

CREATE SCHEMA IF NOT EXISTS tracewell.bronze;
CREATE SCHEMA IF NOT EXISTS tracewell.silver;

-- Every event as received, after redaction, before dedupe (so duplicates are visible here).
CREATE TABLE IF NOT EXISTS tracewell.bronze.ai_events (
  event_id         varchar,
  ts_event         timestamp(6) with time zone,
  ts_sent          timestamp(6) with time zone,
  device_id        varchar,
  os               varchar,
  identity         varchar,
  department       varchar,
  sensor           varchar,
  event_type       varchar,
  asset_ref        varchar,
  asset_version    varchar,
  tool_name        varchar,
  prompt_text      varchar,        -- redacted
  file_name        varchar,
  file_size_bytes  bigint,
  dest_host        varchar,
  bytes_out        bigint,
  sensitive_types  array(varchar),
  sensitive_count  integer,
  ingested_at      timestamp(6) with time zone
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(ts_event)']);

-- Deduplicated, enriched events with the risk profile that was valid when the event was processed.
CREATE TABLE IF NOT EXISTS tracewell.silver.fct_ai_events (
  event_id               varchar,
  ts_event               timestamp(6) with time zone,
  ts_sent                timestamp(6) with time zone,
  device_id              varchar,
  os                     varchar,
  identity               varchar,
  department             varchar,
  sensor                 varchar,
  event_type             varchar,
  asset_ref              varchar,
  asset_id               varchar,
  asset_known            boolean,
  asset_category         varchar,
  risk_score_at_event    double,
  band_at_event          varchar,
  sanctioned_at_event    boolean,
  catalog_valid_from     timestamp(6) with time zone,   -- which SCD2 version of the risk profile was used
  asset_version          varchar,
  tool_name              varchar,
  prompt_text            varchar,                        -- redacted
  file_name              varchar,
  dest_host              varchar,
  bytes_out              bigint,
  sensitive_types        array(varchar),
  sensitive_count        integer,
  policy_id              varchar,
  action                 varchar,                        -- allow | warn | block
  latency_ms             bigint,                         -- ts_sent -> verdict
  processed_at           timestamp(6) with time zone
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(ts_event)', 'bucket(device_id, 16)']);

-- Phase 4: activity sessions per (device, identity). Flink writes on-time sessions (source = 'stream');
-- the Spark late_event_backfill replaces sessions that late events changed (source = 'backfill').
CREATE TABLE IF NOT EXISTS tracewell.silver.fct_sessions (
  session_id        varchar,                          -- sha256(device|identity|start millis)[:16], same in Flink and Spark
  identity          varchar,
  device_id         varchar,
  department        varchar,
  session_start     timestamp(6) with time zone,      -- first event
  session_end       timestamp(6) with time zone,      -- last event
  duration_s        bigint,
  event_count       bigint,
  sensors_seen      array(varchar),
  event_types       array(varchar),
  assets_used       array(varchar),
  unknown_assets    integer,
  max_risk_score    double,
  max_band          varchar,
  sensitive_events  bigint,
  blocks            bigint,
  warns             bigint,
  mb_out            double,
  source            varchar,                          -- stream | backfill
  built_at          timestamp(6) with time zone
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(session_start)']);

-- Phase 5: tamper-evident audit log of every warn/block verdict, written by Flink, append-only.
-- record_hash = sha256(prev_hash || canonical record); verified by spark-jobs/audit/verify_audit_chain.py.
CREATE TABLE IF NOT EXISTS tracewell.silver.audit_log (
  chain_id         integer,                          -- one of 4 parallel chains (by device)
  seq_no           bigint,                           -- 1, 2, 3, ... within the chain, no gaps
  event_id         varchar,
  ts_event         timestamp(6) with time zone,
  identity         varchar,
  device_id        varchar,
  asset_id         varchar,
  action           varchar,                          -- warn | block
  policy_id        varchar,
  band_at_event    varchar,
  sensitive_types  array(varchar),
  prev_hash        varchar,                          -- record_hash of the previous link; 64 zeros for seq_no 1
  record_hash      varchar,
  written_at       timestamp(6) with time zone
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['chain_id']);
