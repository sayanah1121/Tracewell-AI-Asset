"""audit_verify: recompute the tamper-evident audit log's hash chains every night at 04:00.

Flink chains every warn/block verdict into silver.audit_log (record_hash = sha256(prev_hash || record)).
spark-jobs/audit/verify_audit_chain.py checks every hash, link and sequence number, and that the heads
anchored by the previous run are still there. A failure means the evidence was changed: it is not retried
(a retry cannot make an edited row valid) and should page someone.
"""
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

JOBS = "/opt/tracewell/spark-jobs"

with DAG(
    dag_id="audit_verify",
    description="Verify the SHA-256 hash chains of silver.audit_log; failure = critical alert",
    schedule="0 4 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=20)},
    tags=["phase5", "audit"],
):
    SparkSubmitOperator(
        task_id="verify_audit_chain",
        conn_id="spark_default",
        application=f"{JOBS}/audit/verify_audit_chain.py",
        py_files=f"{JOBS}/catalog/common.py",
        name="tracewell-audit-verify",
        conf={"spark.driver.host": "airflow", "spark.driver.bindAddress": "0.0.0.0", "spark.cores.max": "1"},
    )
