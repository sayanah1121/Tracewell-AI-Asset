"""iceberg_maintenance: nightly at 03:30, after catalog_daily (02:00) and before audit_verify (04:00).

One Spark task over every lakehouse table: full compaction of the streaming tables, snapshot expiry by
table retention (30 days for dim_asset_risk, which investigations time-travel into; 2 days for stream
tables; 7 for the rest) and orphan-file cleanup older than 3 days. Per-table results go to
ops.maintenance_log. See spark-jobs/maintenance/iceberg_maintenance.py.
"""
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

JOBS = "/opt/tracewell/spark-jobs"

with DAG(
    dag_id="iceberg_maintenance",
    description="Compact stream tables, expire snapshots, remove orphan files on every Iceberg table",
    schedule="30 3 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "execution_timeout": timedelta(hours=1)},
    tags=["phase5", "maintenance"],
):
    SparkSubmitOperator(
        task_id="iceberg_maintenance",
        conn_id="spark_default",
        application=f"{JOBS}/maintenance/iceberg_maintenance.py",
        py_files=f"{JOBS}/catalog/common.py",
        name="tracewell-iceberg-maintenance",
        conf={"spark.driver.host": "airflow", "spark.driver.bindAddress": "0.0.0.0", "spark.cores.max": "1"},
    )
