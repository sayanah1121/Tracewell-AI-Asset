-- Phase 2 acceptance checks (run with ./tracewell.sh stream-check).

-- 1. Dedupe: bronze keeps every copy; silver must hold exactly one row per event_id.
SELECT
  (SELECT count(*) FROM tracewell.bronze.ai_events)                                   AS bronze_rows,
  (SELECT count(DISTINCT event_id) FROM tracewell.bronze.ai_events)                   AS distinct_events,
  (SELECT count(*) FROM tracewell.silver.fct_ai_events)                                AS silver_rows,
  (SELECT count(*) - count(DISTINCT event_id) FROM tracewell.silver.fct_ai_events)    AS silver_duplicates;

-- 2. Privacy: no raw secrets, card numbers, emails, phone numbers or PAN/Aadhaar may reach storage.
SELECT layer, count_if(regexp_like(prompt_text, 'AKIA[0-9A-Z]{16}|sk-[A-Za-z0-9]{20,}|gh[pousr]_[A-Za-z0-9]{30,}')) AS raw_secrets,
       count_if(regexp_like(prompt_text, '\b\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}\b|\b3[47]\d{13}\b'))           AS raw_cards,
       count_if(regexp_like(prompt_text, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'))                   AS raw_emails,
       count_if(regexp_like(prompt_text, '\b[A-Z]{5}\d{4}[A-Z]\b|\b[2-9]\d{3} \d{4} \d{4}\b|\+91 \d{5} \d{5}')) AS raw_gov_ids_phones,
       count_if(sensitive_count > 0)                                                                          AS redacted_events
FROM (SELECT 'bronze' AS layer, prompt_text, sensitive_count FROM tracewell.bronze.ai_events
      UNION ALL SELECT 'silver', prompt_text, sensitive_count FROM tracewell.silver.fct_ai_events)
GROUP BY layer ORDER BY layer;

-- 3. Verdicts by policy.
SELECT action, coalesce(policy_id, '-') AS policy_id, count(*) AS events
FROM tracewell.silver.fct_ai_events GROUP BY 1, 2 ORDER BY 1, 2;

-- 4. Enrichment: share of events resolved against the risk catalog.
SELECT count(*) AS events, count_if(asset_known) AS resolved,
       round(100.0 * count_if(NOT asset_known) / count(*), 2) AS unknown_pct,
       count(DISTINCT asset_id) FILTER (WHERE asset_known) AS distinct_known_assets
FROM tracewell.silver.fct_ai_events;

-- 5. Latency from the device sending the event to Flink's verdict (offline-buffered events excluded:
--    their ts_sent is the flush time, so this measures pipeline latency only).
SELECT approx_percentile(latency_ms, 0.5) AS p50_ms, approx_percentile(latency_ms, 0.95) AS p95_ms,
       approx_percentile(latency_ms, 0.99) AS p99_ms, max(latency_ms) AS max_ms
FROM tracewell.silver.fct_ai_events;

-- 6. Late data: events whose device time is far behind their send time (offline devices flushing later).
SELECT count_if(ts_sent - ts_event > INTERVAL '1' MINUTE) AS sent_over_1min_late,
       max(ts_sent - ts_event) AS max_lateness
FROM tracewell.silver.fct_ai_events;

-- 7. Scenario evidence: what got blocked, and why.
SELECT ts_event, identity, event_type, asset_id, band_at_event, sanctioned_at_event, sensitive_types, policy_id
FROM tracewell.silver.fct_ai_events WHERE action = 'block' ORDER BY ts_event DESC LIMIT 12;
