-- Phase 4 acceptance checks for silver.fct_sessions (run with ./tracewell.sh session-check).

-- 1. Sessions by source: Flink windows vs Spark backfill after late events.
SELECT source, count(*) AS sessions, sum(event_count) AS events, round(avg(duration_s) / 60.0, 1) AS avg_minutes
FROM tracewell.silver.fct_sessions GROUP BY source ORDER BY source;

-- 2. No two sessions of the same person on the same device may overlap (must be 0).
SELECT count(*) AS overlapping_session_pairs
FROM tracewell.silver.fct_sessions a JOIN tracewell.silver.fct_sessions b
  ON a.device_id = b.device_id AND a.identity = b.identity AND a.session_id < b.session_id
 AND a.session_start <= b.session_end AND b.session_start <= a.session_end;

-- 3. Every settled event is counted in exactly one session (must be 0 mismatched pairs).
--    "Settled" follows stream time, not the wall clock. Flink fires a session window when the watermark (newest
--    event - 5 min) passes the session's last event + the 10-min gap; when events stop (the simulator ends), the
--    watermark stops too and recent sessions stay open until new events arrive. So per (device, person) the
--    events are split into runs by the same gap rule, and everything from the first run that has not fired yet
--    is left out. For the rest, the events in silver must equal the sum of the sessions' event counts; with no
--    overlaps (check 2) each event then sits in exactly one session. No range joins: one ran Trino's 1 GB heap
--    out of memory.
WITH stream AS (SELECT max(ts_event) - INTERVAL '5' MINUTE AS watermark FROM tracewell.silver.fct_ai_events),
e AS (SELECT device_id, identity, ts_event,
             CASE WHEN ts_event - lag(ts_event) OVER w >= INTERVAL '10' MINUTE THEN 1 ELSE 0 END AS new_run
      FROM tracewell.silver.fct_ai_events WINDOW w AS (PARTITION BY device_id, identity ORDER BY ts_event)),
r AS (SELECT device_id, identity, ts_event,
             sum(new_run) OVER (PARTITION BY device_id, identity ORDER BY ts_event ROWS UNBOUNDED PRECEDING) AS run
      FROM e),
runs AS (SELECT device_id, identity, run, min(ts_event) AS run_start, max(ts_event) AS run_end FROM r GROUP BY 1, 2, 3),
settled AS (SELECT device_id, identity,
                   coalesce(min(CASE WHEN run_end + INTERVAL '10' MINUTE > stream.watermark THEN run_start END),
                            TIMESTAMP '9999-12-31 00:00:00 UTC') AS until
            FROM runs, stream GROUP BY 1, 2),
ev AS (SELECT x.device_id, x.identity, count(*) AS n
       FROM tracewell.silver.fct_ai_events x JOIN settled u ON u.device_id = x.device_id AND u.identity = x.identity
       WHERE x.ts_event < u.until GROUP BY 1, 2),
ss AS (SELECT x.device_id, x.identity, sum(x.event_count) AS n
       FROM tracewell.silver.fct_sessions x JOIN settled u ON u.device_id = x.device_id AND u.identity = x.identity
       WHERE x.session_start < u.until GROUP BY 1, 2)
SELECT count(*) AS pairs_checked, sum(ev.n) AS events_checked,
       count_if(coalesce(ss.n, 0) <> ev.n) AS pairs_with_missing_or_extra_events
FROM ev LEFT JOIN ss ON ss.device_id = ev.device_id AND ss.identity = ev.identity;

-- 4. Late events: what Flink sent to the side output in the last 48 h, by how late they were.
--    (Read from Kafka by late_event_backfill; here the per-device result of the repair.)
SELECT device_id, count(*) AS backfilled_sessions, sum(event_count) AS events, min(session_start) AS first, max(session_end) AS last
FROM tracewell.silver.fct_sessions WHERE source = 'backfill' AND built_at > current_timestamp - INTERVAL '2' DAY
  AND session_start > current_timestamp - INTERVAL '2' DAY
GROUP BY device_id ORDER BY backfilled_sessions DESC LIMIT 5;
