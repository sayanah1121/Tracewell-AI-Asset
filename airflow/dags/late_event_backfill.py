"""late_event_backfill: repair activity sessions that late events changed, every hour at :45.

Flink sends events that arrive after their session window closed to the `late-events` topic; this DAG
re-sessionizes those devices from silver.fct_ai_events and swaps the result into silver.fct_sessions
in one MERGE (see spark-jobs/sessions/late_event_backfill.py). Runs at :45 so it never overlaps gold_marts (:15).
"""
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

JOBS = "/opt/tracewell/spark-jobs"

with DAG(
    dag_id="late_event_backfill",
    description="Re-sessionize devices with late events and merge the repaired sessions into silver.fct_sessions",
    schedule="45 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "execution_timeout": timedelta(minutes=20)},
    tags=["phase4", "sessions"],
):
    SparkSubmitOperator(
        task_id="backfill_late_events",
        conn_id="spark_default",
        application=f"{JOBS}/sessions/late_event_backfill.py",
        application_args=["--hours", "48", "--gap-minutes", "10"],
        py_files=f"{JOBS}/catalog/common.py,{JOBS}/sessions/sessionize.py",
        name="tracewell-sessions-backfill_late_events",
        conf={"spark.driver.host": "airflow", "spark.driver.bindAddress": "0.0.0.0", "spark.cores.max": "1"},
    )
