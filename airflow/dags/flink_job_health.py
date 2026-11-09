"""flink_job_health: every 15 minutes, check the streaming job through the Flink REST API.

  * tracewell-stream is RUNNING;
  * its newest completed checkpoint is younger than MAX_CHECKPOINT_AGE (checkpoints run every 20 s, and
    Iceberg only commits on a checkpoint, so a stale checkpoint means silver stopped growing);
  * the consumer group's lag on ai-events-raw is below MAX_LAG: topic end offsets minus the offsets Flink
    commits on each checkpoint. (Flink's pendingRecords metric and the Redpanda Console API both reported
    nothing while the real lag grew to thousands, so neither is used.)

Any failed check fails the task (the alert). If the job is not running and the Airflow Variable
flink_auto_resubmit is "true", the task first resubmits the job from the newest retained checkpoint of
the last run (the jar is uploaded from /opt/tracewell/flink-jobs), the same as ./tracewell.sh submit-flink.
"""
import time
from datetime import datetime, timedelta

import requests
from airflow.sdk import DAG, Variable, task
from confluent_kafka import Consumer, TopicPartition

FLINK = "http://flink-jobmanager:8081"
JOB = "tracewell-stream"
JAR = "/opt/tracewell/flink-jobs/tracewell-stream/target/tracewell-stream.jar"
ENTRY = "ai.tracewell.stream.TracewellStreamJob"
MAX_CHECKPOINT_AGE = timedelta(minutes=2)
MAX_LAG = 50_000
BOOTSTRAP = "redpanda:9092"


def get(path: str):
    r = requests.get(f"{FLINK}{path}", timeout=15)
    r.raise_for_status()
    return r.json()


def kafka_lag(topic: str = "ai-events-raw", group: str = JOB) -> int:
    """Records the group is behind: end offset minus committed offset, summed over partitions."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    try:
        parts = [TopicPartition(topic, p) for p in c.list_topics(topic, timeout=10).topics[topic].partitions]
        committed = c.committed(parts, timeout=10)
        return sum(c.get_watermark_offsets(tp, timeout=10)[1] - max(tp.offset, 0) for tp in committed)
    finally:
        c.close()


def jobs_by_newest() -> list[dict]:
    return sorted((j for j in get("/jobs/overview")["jobs"] if j["name"] == JOB), key=lambda j: -j["start-time"])


def resubmit(last: dict | None) -> str:
    """Start the job again from the last run's newest retained checkpoint; returns the new job id."""
    path = None
    if last:
        completed = (get(f"/jobs/{last['jid']}/checkpoints").get("latest") or {}).get("completed") or {}
        path = completed.get("external_path")
    if not path:
        raise RuntimeError("no retained checkpoint known to the JobManager; resubmit by hand: ./tracewell.sh submit-flink")
    with open(JAR, "rb") as f:
        up = requests.post(f"{FLINK}/jars/upload", files={"jarfile": ("tracewell-stream.jar", f, "application/x-java-archive")},
                           timeout=120)
    up.raise_for_status()
    jar_id = up.json()["filename"].rsplit("/", 1)[-1]
    run = requests.post(f"{FLINK}/jars/{jar_id}/run", json={"entryClass": ENTRY, "savepointPath": path}, timeout=120)
    run.raise_for_status()
    print(f"resubmitted {JOB} from {path} as job {run.json()['jobid']}")
    return run.json()["jobid"]


with DAG(
    dag_id="flink_job_health",
    description="Flink job running, checkpoints fresh, Kafka backlog bounded; optional resubmit",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=5)},
    tags=["phase5", "health"],
):
    @task
    def check_flink_job() -> dict:
        jobs = jobs_by_newest()
        running = [j for j in jobs if j["state"] == "RUNNING"]
        if not running:
            if Variable.get("flink_auto_resubmit", default="false").lower() != "true":
                last_state = jobs[0]["state"] if jobs else "never submitted"
                raise RuntimeError(f"{JOB} is not running (last state: {last_state}); "
                                   "set Variable flink_auto_resubmit=true to restart it automatically")
            resubmit(jobs[0] if jobs else None)
            time.sleep(60)  # let it restore and complete a first checkpoint before the checks below
            running = [j for j in jobs_by_newest() if j["state"] == "RUNNING"]
            if not running:
                raise RuntimeError(f"{JOB} was resubmitted but is not RUNNING")
        jid = running[0]["jid"]

        latest = (get(f"/jobs/{jid}/checkpoints").get("latest") or {}).get("completed")
        age = timedelta(milliseconds=time.time() * 1000 - latest["latest_ack_timestamp"]) if latest else None
        lag = kafka_lag()

        status = {"job_id": jid, "checkpoint_age_s": age.total_seconds() if age else None, "kafka_lag": lag}
        print(status)
        problems = []
        if age is None or age > MAX_CHECKPOINT_AGE:
            problems.append(f"newest checkpoint is {age or 'missing'} old (limit {MAX_CHECKPOINT_AGE})")
        if lag > MAX_LAG:
            problems.append(f"{lag} records behind on ai-events-raw (limit {MAX_LAG})")
        if problems:
            raise RuntimeError("; ".join(problems))
        return status

    check_flink_job()
