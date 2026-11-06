"""Phase 0 smoke DAG: Airflow submits a Spark job that appends to the shared Iceberg table."""
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_spark_iceberg",
    description="Smoke test: SparkSubmitOperator -> Spark cluster -> Iceberg",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["phase0", "smoke"],
):
    SparkSubmitOperator(
        task_id="append_from_airflow",
        conn_id="spark_default",
        application="/opt/tracewell/spark-jobs/smoke/smoke_spark.py",
        application_args=["append", "airflow"],
        name="tracewell-smoke-airflow",
        conf={
            # Driver runs inside the Airflow container; executors call back to it.
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
        },
    )
