#!/usr/bin/env bash
# Tracewell helper. Run from Git Bash / WSL / macOS / Linux.
#   ./tracewell.sh up [all|core|batch|bi|stream ...]
#                                           start profiles (default: core batch = streaming + scheduled batch;
#                                           Trino/Superset (bi) start on demand, the simulator (stream) when asked)
#   ./tracewell.sh down                     stop everything (keeps data volumes)
#   ./tracewell.sh reset                    stop everything AND delete all data volumes
#   ./tracewell.sh ps | logs <service>      status / logs
#   ./tracewell.sh wait                     wait until the running services answer
#   ./tracewell.sh smoke                    Phase 0 smoke test (Spark + Flink + Airflow + Trino on one Iceberg table)
#   ./tracewell.sh test                     unit tests (pytest inside the Spark container)
#   ./tracewell.sh catalog                  run the catalog_daily DAG once, now (Phase 1 risk catalog)
#   ./tracewell.sh init                     Phase 2: create topics, register schemas, create stream tables, publish policies
#   ./tracewell.sh policies                 publish policies/*.yaml to Flink (applies within seconds)
#   ./tracewell.sh build-flink              build + unit-test the Flink job jar (Maven in Docker)
#   ./tracewell.sh submit-flink [--fresh | --allow-non-restored]
#                                           (re)submit tracewell-stream from its latest checkpoint; --fresh = from scratch;
#                                           --allow-non-restored = after adding operators (only with the stream quiet)
#   ./tracewell.sh simulate [args]          run the fleet simulator, e.g. --rate 50 --duration 300 --scenarios rogue_mcp
#   ./tracewell.sh stream-check             Phase 2 acceptance checks (dedupe, no raw PII, verdicts, latency)
#   ./tracewell.sh gold                     Phase 3: run the gold_marts DAG once, now (compaction, marts, dq_gold)
#   ./tracewell.sh dashboards               Phase 3: create or update the Superset dashboards (http://localhost:8089)
#   ./tracewell.sh backfill [--full]        Phase 4: repair sessions after late events (--full: sessionize all of silver)
#   ./tracewell.sh investigate --session ID | --event ID   Phase 4: incident timeline, then-vs-now risk, blast radius
#   ./tracewell.sh session-check            Phase 4 acceptance checks (no overlaps, every event in exactly one session)
#   ./tracewell.sh audit-verify             Phase 5: recompute the audit log's hash chains (fails if any row was changed)
#   ./tracewell.sh demo-tamper              Phase 5: edit one audit row, show audit-verify catch it, then restore it
#   ./tracewell.sh maintenance              Phase 5: compact, expire snapshots, remove orphans on every Iceberg table
#   ./tracewell.sh demo-crash [seconds]     Phase 5: kill the Flink TaskManager mid-stream; counts must match exactly
#   ./tracewell.sh demo-schema              Phase 5: add an event field (Avro v2 + Iceberg column) under the running job
#   ./tracewell.sh bench [rates...]         Phase 5: throughput and latency at each simulator rate (default 50 200 500 /s)
set -euo pipefail
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1   # stop Git Bash rewriting /opt/... container paths into Windows paths

ALL_PROFILES=(core batch bi stream)
compose_all() { docker compose $(printf -- '--profile %s ' "${ALL_PROFILES[@]}") "$@"; }

[[ -f .env ]] || { echo "Missing .env: copy .env.example to .env and set the secrets."; exit 1; }

wait_http() {  # name url [tries]
  local name=$1 url=$2 tries=${3:-60}
  printf '  waiting for %-18s' "$name"
  for _ in $(seq 1 "$tries"); do
    if curl -fsS "$url" >/dev/null 2>&1; then echo " up"; return 0; fi
    sleep 3; printf '.'
  done
  echo " NOT RESPONDING ($url)"; return 1
}

# Newest completed checkpoint (one with a _metadata file) of any tracewell-stream run, as s3://... path.
latest_checkpoint() {
  docker run --rm --network tracewell_default -e AWS_ACCESS_KEY_ID="$(grep ^S3_ACCESS_KEY .env | cut -d= -f2)"     -e AWS_SECRET_ACCESS_KEY="$(grep ^S3_SECRET_KEY .env | cut -d= -f2)" -e AWS_DEFAULT_REGION=us-east-1     amazon/aws-cli:2.38.4 --endpoint-url http://seaweedfs:8333 s3 ls --recursive s3://checkpoints/flink/ 2>/dev/null     | grep '/chk-[0-9]*/_metadata$' | sort -k1,2 | tail -1 | awk '{print $4}' | sed 's|/_metadata$||; s|^|s3://checkpoints/|'
}

running() { compose_all ps --status running --services 2>/dev/null | grep -qx "$1"; }

# Start a BI service on first use, so the always-on stack can leave Trino and Superset off (8 GB laptops).
need() {  # service
  running "$1" && return 0
  echo "starting $1 on demand (stop it again with: docker compose --profile bi stop $1)" >&2
  docker compose --profile bi up -d "$1" >/dev/null 2>&1
  case $1 in
    trino)    wait_http trino http://localhost:8085/v1/info >&2
              until compose_all exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1; do sleep 3; done ;;
    superset) wait_http superset http://localhost:8089/health 100 >&2 ;;
  esac
}

trino_cli() { need trino; compose_all exec -T trino trino "$@"; }

cmd_wait() {
  running iceberg-rest      && wait_http iceberg-rest  http://localhost:8181/v1/config
  running flink-jobmanager  && wait_http flink         http://localhost:8081/overview
  running redpanda-console  && wait_http redpanda-console http://localhost:8080
  running spark-master      && wait_http spark-master  http://localhost:8082
  running airflow           && wait_http airflow       http://localhost:8088/api/v2/monitor/health 100
  running trino             && wait_http trino         http://localhost:8085/v1/info
  running superset          && wait_http superset      http://localhost:8089/health 100
  return 0
}

step() { echo; echo "── $* ─────────────────────────────────────────────"; }

# spark-submit from the Airflow container (client mode), 1 core; extra args after the script go to the job.
spark_job() {  # py-files script [args...]
  local py_files=$1 script=$2; shift 2
  compose_all exec -T airflow spark-submit --master spark://spark-master:7077 --conf spark.driver.host=airflow \
    --conf spark.driver.bindAddress=0.0.0.0 --conf spark.cores.max=1 --py-files "$py_files" "$script" "$@"
}

audit_verify() {
  spark_job /opt/tracewell/spark-jobs/catalog/common.py /opt/tracewell/spark-jobs/audit/verify_audit_chain.py "$@" 2>&1 \
    | grep -E '\[catalog\]|Error|Exception' | sed -E 's/^\[catalog\] [0-9:]+ //'
  return "${PIPESTATUS[0]}"
}

trino_q() { trino_cli --output-format=TSV --execute "$1"; }

kafka_counts() { compose_all run --rm -T --no-deps simulator python kafka_counts.py "$@" 2>/dev/null | tail -1; }

# "<job id> <restarts>" of the running tracewell-stream job.
flink_job() {
  curl -fsS http://localhost:8081/jobs/overview | python -c "
import json, sys, urllib.request
jobs = [j for j in json.load(sys.stdin)['jobs'] if j['name'] == 'tracewell-stream' and j['state'] == 'RUNNING']
if jobs:
    m = json.load(urllib.request.urlopen('http://localhost:8081/jobs/%s/metrics?get=numRestarts' % jobs[0]['jid']))
    print(jobs[0]['jid'], m[0]['value'] if m else '?')"
}

# Records the Flink job is behind on ai-events-raw: end offsets minus the offsets Flink commits on each
# checkpoint. (Flink's pendingRecords metric and the Redpanda Console API both showed nothing under load.)
# rpk runs inside the Redpanda container already up, so sampling every few seconds adds no load.
kafka_lag() { compose_all exec -T redpanda rpk group describe tracewell-stream 2>/dev/null | awk '/^TOTAL-LAG/ {print $2}'; }

cmd_smoke() {
  for s in iceberg-rest flink-jobmanager flink-taskmanager spark-master spark-worker airflow trino; do
    running "$s" || { echo "Service '$s' is not running. Start it with: ./tracewell.sh up core batch bi"; exit 1; }
  done
  cmd_wait

  step "1/5 Spark creates the Iceberg table and writes 2 rows"
  compose_all exec -T spark-master spark-submit --master spark://spark-master:7077 \
    /opt/tracewell/spark-jobs/smoke/smoke_spark.py init

  step "2/5 Flink SQL appends 2 rows to the same table and reads it back"
  compose_all exec -T flink-jobmanager /opt/flink/bin/sql-client.sh \
    -f /opt/tracewell/flink-jobs/smoke/smoke_flink.sql

  step "3/5 Airflow runs the hello_spark_iceberg DAG (SparkSubmitOperator)"
  compose_all exec -T airflow airflow dags reserialize >/dev/null 2>&1 || true
  compose_all exec -T airflow airflow dags test hello_spark_iceberg

  step "4/5 Trino reads every engine's rows"
  local out
  out=$(trino_cli --output-format=ALIGNED \
    --execute "SELECT engine, count(*) AS n FROM tracewell.smoke.engine_writes GROUP BY engine ORDER BY engine")
  echo "$out"
  for e in airflow flink spark; do
    grep -q "$e" <<<"$out" || { echo "[smoke] FAILED: Trino does not see rows from $e"; exit 1; }
  done

  step "5/5 Spark verifies rows and Iceberg snapshots"
  compose_all exec -T spark-master spark-submit --master spark://spark-master:7077 \
    /opt/tracewell/spark-jobs/smoke/smoke_spark.py verify

  echo; echo "✅ Phase 0 smoke test PASSED"
}

case "${1:-help}" in
  up)
    shift
    profiles=("$@"); [[ ${#profiles[@]} -eq 0 ]] && profiles=(core batch)
    [[ "${profiles[0]}" == "all" ]] && profiles=("${ALL_PROFILES[@]}")
    docker compose $(printf -- '--profile %s ' "${profiles[@]}") up -d --build
    cmd_wait ;;
  down)   compose_all down ;;
  reset)  compose_all down -v ;;
  ps)     compose_all ps ;;
  logs)   shift; compose_all logs -f --tail=200 "$@" ;;
  wait)   cmd_wait ;;
  smoke)  cmd_smoke ;;
  init)
    compose_all run --rm simulator python init_topics.py
    trino_cli -f /dev/stdin < sql/ddl/stream_tables.sql
    compose_all run --rm simulator python publish_policies.py ;;
  policies) compose_all run --rm simulator python publish_policies.py ;;
  build-flink)
    docker run --rm -v "$PWD/flink-jobs/tracewell-stream:/src" -v tracewell_maven-cache:/root/.m2 -w /src       maven:3.9.13-eclipse-temurin-17 mvn -q -B package ;;
  submit-flink)
    for id in $(compose_all exec -T flink-jobmanager flink list -r 2>/dev/null | grep -oE '[0-9a-f]{32} : tracewell-stream' | cut -d' ' -f1); do
      echo "cancelling running tracewell-stream job $id"; compose_all exec -T flink-jobmanager flink cancel "$id" >/dev/null
    done
    resume=()
    if [[ "${2:-}" != "--fresh" ]]; then
      cp=$(latest_checkpoint)
      [[ -n "$cp" ]] && { echo "resuming from $cp"; resume=(-s "$cp"); }
      [[ -n "$cp" && "${2:-}" == "--allow-non-restored" ]] && resume+=(--allowNonRestoredState)
    fi
    compose_all exec -T flink-jobmanager flink run -d "${resume[@]}" /opt/tracewell/flink-jobs/tracewell-stream/target/tracewell-stream.jar ;;
  simulate) shift; compose_all run --rm simulator python simulator.py "$@" ;;
  stream-check) trino_cli --output-format=ALIGNED -f /dev/stdin < sql/checks/stream_checks.sql ;;
  session-check) trino_cli --output-format=ALIGNED -f /dev/stdin < sql/checks/session_checks.sql ;;
  test)   compose_all exec -T -w /opt/tracewell spark-master python -m pytest -q -p no:cacheprovider tests ;;
  catalog)
    compose_all exec -T airflow airflow dags reserialize >/dev/null 2>&1 || true
    compose_all exec -T airflow airflow dags test catalog_daily ;;
  gold)
    compose_all exec -T airflow airflow dags reserialize >/dev/null 2>&1 || true
    compose_all exec -T airflow airflow dags test gold_marts ;;
  dashboards) shift; need trino; need superset; compose_all exec -T superset python - "$@" < dashboards/build_dashboards.py ;;
  backfill)
    shift
    compose_all exec -T airflow spark-submit --master spark://spark-master:7077 --conf spark.driver.host=airflow       --conf spark.driver.bindAddress=0.0.0.0 --conf spark.cores.max=1       --py-files /opt/tracewell/spark-jobs/catalog/common.py,/opt/tracewell/spark-jobs/sessions/sessionize.py       /opt/tracewell/spark-jobs/sessions/late_event_backfill.py "$@" 2>&1 | grep -E '\[catalog\]|Error|Exception' ;;
  investigate)
    shift
    compose_all exec -T airflow spark-submit --master spark://spark-master:7077 --conf spark.driver.host=airflow       --conf spark.driver.bindAddress=0.0.0.0 --conf spark.cores.max=1       --py-files /opt/tracewell/spark-jobs/catalog/common.py /opt/tracewell/spark-jobs/investigate/investigate.py "$@" 2>&1       | grep -E '\[catalog\]|Error|Exception|not found'
    # point the Investigation dashboard's default at the new investigation
    need trino; need superset; compose_all exec -T superset python - investigation < dashboards/build_dashboards.py | tail -1 ;;
  audit-verify) audit_verify ;;
  bench)
    shift; rates=("$@"); [[ ${#rates[@]} -eq 0 ]] && rates=(50 200 500)
    compose_all stop simulator >/dev/null 2>&1 || true
    printf '%-8s %-9s %-10s %-8s %-8s %-8s %-8s %-12s\n' "rate/s" "events" "events/s" "p50 ms" "p95 ms" "p99 ms" "late %" "max lag"
    for rate in "${rates[@]}"; do
      sleep 30   # let the previous run drain
      t0=$(trino_q "SELECT format_datetime(current_timestamp AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss.SSS')")
      compose_all run --rm -T simulator python simulator.py --rate "$rate" --duration 90 >/dev/null 2>&1 &
      sim=$!; backlog=0
      while kill -0 "$sim" 2>/dev/null; do
        b=$(kafka_lag); [[ -n "$b" && "$b" -gt "$backlog" ]] && backlog=$b; sleep 5
      done
      for _ in $(seq 1 120); do b=$(kafka_lag); [[ -n "$b" && "$b" -gt "$backlog" ]] && backlog=$b; [[ "$b" == "0" ]] && break; sleep 5; done
      sleep 45   # two checkpoints: everything processed is committed
      IFS=$'\t' read -r n eps p50 p95 p99 late < <(trino_q "SELECT count(*),
          round(count(*) / greatest(1, date_diff('millisecond', min(processed_at), max(processed_at)) / 1000.0)),
          approx_percentile(latency_ms, 0.5), approx_percentile(latency_ms, 0.95), approx_percentile(latency_ms, 0.99),
          format('%.2f', 100.0 * count_if(date_diff('second', ts_event, processed_at) > 300) / count(*))
        FROM tracewell.silver.fct_ai_events WHERE processed_at >= TIMESTAMP '$t0 UTC'")
      printf '%-8s %-9s %-10s %-8s %-8s %-8s %-8s %-12s\n' "$rate" "$n" "$eps" "$p50" "$p95" "$p99" "$late" "$backlog"
    done ;;
  demo-schema)
    step "1. The registry's compatibility check for v2 (adds optional agent_parent_id, default null)"
    compose_all run --rm -T --no-deps simulator python -c "
from tracewell_kafka import check_compatibility
ok = check_compatibility('ai-events-raw-value', 'ai_event_v2.avsc')
print('compatible with the current version:', ok)
raise SystemExit(0 if ok else 1)" 2>/dev/null | tail -1
    read -r job0 restarts0 < <(flink_job)
    [[ -n "${job0:-}" ]] || { echo "tracewell-stream is not running"; exit 1; }
    dlq0=$(kafka_counts offsets ai-events-dlq)
    t0=$(trino_q "SELECT format_datetime(current_timestamp AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss.SSS')")
    echo "running job $job0, restarts so far: $restarts0"
    step "2. Add the column to the lakehouse (Iceberg: metadata only, no data rewritten)"
    for t in bronze.ai_events silver.fct_ai_events; do
      if trino_q "SHOW COLUMNS FROM tracewell.$t" | grep -q '^agent_parent_id'; then echo "tracewell.$t already has agent_parent_id"
      else trino_q "ALTER TABLE tracewell.$t ADD COLUMN agent_parent_id varchar"; fi
    done
    step "3. Send 60 s of v2 events to the running job (no redeploy)"
    compose_all run --rm -T simulator python simulator.py --schema-v2 --rate 40 --duration 60 2>&1 \
      | grep -E '^producing with|^\[ *[0-9]+s\]' | sed -n '1p;$p'
    sleep 45
    step "4. Is the job still fine?"
    read -r job1 restarts1 < <(flink_job)
    dlq=$(kafka_counts count ai-events-dlq "$dlq0")
    rows=$(trino_q "SELECT count(*) FROM tracewell.silver.fct_ai_events WHERE processed_at >= TIMESTAMP '$t0 UTC'")
    printf '  %-36s %s\n' "job id before / after" "$job0 / ${job1:-not running}" "restarts before / after" "$restarts0 / ${restarts1:-?}" \
      "new dead-letter records" "$dlq" "v2 events landed in silver" "$rows"
    if [[ "$job1" == "$job0" && "$restarts1" == "$restarts0" && "$dlq" == "0" && "$rows" -gt 0 ]]; then
      echo "PASS  the running job reads v2 events (it ignores the new field until a job version maps it)"
    else
      echo "FAIL"; exit 1
    fi ;;
  demo-crash)
    dur=${2:-120}
    step "0. Quiet the stream (stop the continuous simulator, let the last checkpoint commit)"
    compose_all stop simulator >/dev/null 2>&1 || true
    sleep 45
    t0=$(trino_q "SELECT format_datetime(current_timestamp AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss.SSS')")
    raw0=$(kafka_counts offsets ai-events-raw); verdicts0=$(kafka_counts offsets verdicts)
    echo "start $t0 UTC; ai-events-raw end offsets $raw0"
    step "1. Send events for ${dur}s (duplicates, offline flushes and clock skew included)"
    log=$(mktemp); compose_all run --rm -T simulator python simulator.py --rate 50 --duration "$dur" >"$log" 2>&1 &
    sim=$!
    sleep $((dur / 3))
    step "2. Kill the Flink TaskManager mid-stream ($(date -u +%H:%M:%S) UTC)"
    docker kill tracewell-flink-taskmanager-1 >/dev/null
    sleep 10
    docker start tracewell-flink-taskmanager-1 >/dev/null 2>&1 || true
    echo "TaskManager restarted; Flink restores the job from its last completed checkpoint"
    wait "$sim" || true
    grep -E '^\[ *[0-9]+s\]' "$log" | tail -1; rm -f "$log"
    step "3. Wait until Flink has caught up, then for two more checkpoints to commit"
    for _ in $(seq 1 60); do [[ "$(kafka_lag)" == "0" ]] && break; sleep 5; done
    sleep 25
    step "4. Compare what was sent with what landed"
    produced=$(kafka_counts count ai-events-raw "$raw0"); verdicts=$(kafka_counts count verdicts "$verdicts0")
    IFS=$'\t' read -r bronze unique silver silver_ids < <(trino_q "SELECT
      (SELECT count(*) FROM tracewell.bronze.ai_events WHERE ingested_at >= TIMESTAMP '$t0 UTC'),
      (SELECT count(DISTINCT event_id) FROM tracewell.bronze.ai_events WHERE ingested_at >= TIMESTAMP '$t0 UTC'),
      (SELECT count(*) FROM tracewell.silver.fct_ai_events WHERE processed_at >= TIMESTAMP '$t0 UTC'),
      (SELECT count(DISTINCT event_id) FROM tracewell.silver.fct_ai_events WHERE processed_at >= TIMESTAMP '$t0 UTC')")
    printf '  %-48s %8s\n' "records sent to ai-events-raw (incl. duplicates)" "$produced" \
      "bronze rows (every copy, exactly once)" "$bronze" "distinct event ids sent" "$unique" \
      "silver rows (deduplicated)" "$silver" "distinct event ids in silver" "$silver_ids" \
      "committed verdicts on Kafka" "$verdicts"
    if [[ "$bronze" == "$produced" && "$silver" == "$unique" && "$silver_ids" == "$silver" && "$verdicts" == "$silver" ]]; then
      echo "PASS  exactly-once: nothing lost, nothing duplicated across the crash"
    else
      echo "FAIL  counts differ"; exit 1
    fi
    audit_verify ;;
  maintenance)
    shift
    # (commit retries against Flink's 20 s appends are logged as CommitFailedException warnings; the job retries them)
    spark_job /opt/tracewell/spark-jobs/catalog/common.py /opt/tracewell/spark-jobs/maintenance/iceberg_maintenance.py "$@" 2>&1       | grep -E '\[catalog\]|Error|Exception' | grep -vE 'CommitFailedException|^\s+at |withInternalError' ;;
  demo-tamper)
    step "1. Verify the untouched audit log"
    audit_verify || { echo "The log is already broken; fix that first."; exit 1; }
    IFS=$'\t' read -r chain seq event < <(trino_q "SELECT chain_id, seq_no, event_id FROM tracewell.silver.audit_log
      WHERE action = 'block' ORDER BY written_at DESC LIMIT 1")
    [[ -n "${seq:-}" ]] || { echo "No block verdicts in the audit log yet: run ./tracewell.sh simulate first."; exit 1; }
    step "2. Tamper: rewrite the block of event $event (chain $chain, seq_no $seq) as an allow"
    restore="UPDATE tracewell.silver.audit_log SET action = 'block' WHERE chain_id = $chain AND seq_no = $seq"
    trap 'trino_q "$restore" >/dev/null' EXIT          # never leave the log tampered, whatever fails below
    trino_q "UPDATE tracewell.silver.audit_log SET action = 'allow' WHERE chain_id = $chain AND seq_no = $seq"
    step "3. Verify again (must FAIL)"
    if audit_verify; then echo "NOT DETECTED: the verifier missed the edit"; exit 1; fi
    echo "Detected: the edited row no longer matches its record_hash."
    step "4. The edit is also on record in the table's Iceberg history"
    trino_cli --output-format=ALIGNED --execute "SELECT committed_at, operation,
      element_at(summary, 'trino_user') AS by_user, element_at(summary, 'added-position-deletes') AS rows_deleted,
      element_at(summary, 'added-records') AS rows_added
      FROM tracewell.silver.\"audit_log\$snapshots\" ORDER BY committed_at DESC LIMIT 2"
    step "5. Restore the original value and verify"
    trino_q "$restore"; trap - EXIT
    audit_verify ;;
  *)      sed -n '2,/^set /p' "$0" | grep '^#' ;;
esac
