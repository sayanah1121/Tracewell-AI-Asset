"""gold_marts: rebuild the dashboard tables from silver every hour.

  compact_stream_tables ─┐   ┌─ shadow_ai ───────────┐
                         │   ├─ fleet_risk_posture ──┤
                         ├───┼─ policy_hits ─────────┤
  load_reference ────────┘   ├─ ai_roi ──────────────┼─ dq_gold
                             ├─ compliance_evidence ─┤
                             ├─ pipeline_health ─────┤
                             └─ ocsf_findings ───────┘

compact_stream_tables merges the tiny files Flink's 20 s commits leave behind; without it every mart
opened thousands of ~13 KB files and took minutes for a few rows.

Each mart rebuilds today and yesterday (UTC) and overwrites only those partitions, so a rerun gives the
same rows and late events are folded in by the next run. Superset reads the tables live through Trino.
"""
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, cross_downstream

JOBS = "/opt/tracewell/spark-jobs"
MARTS = ["shadow_ai", "fleet_risk_posture", "policy_hits", "ai_roi", "compliance_evidence", "pipeline_health",
         "ocsf_findings"]


def spark_task(task_id: str, script: str, args: list[str], **kwargs) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=f"{JOBS}/gold/{script}",
        application_args=args,
        py_files=f"{JOBS}/catalog/common.py,{JOBS}/gold/marts.py",
        name=f"tracewell-gold-{task_id}",
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.cores.max": "1",
        },
        **kwargs,
    )


with DAG(
    dag_id="gold_marts",
    description="Hourly gold marts for the dashboards: shadow AI, risk posture, policy hits, AI ROI, compliance, pipeline health",
    schedule="15 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    # One Spark app at a time: on the 8 GB laptop VM two drivers + two executors next to the always-on stack
    # exhausted RAM and swap, and each mart took ~5.5 min instead of ~1.5 min when run alone.
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=50),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["phase3", "gold"],
):
    compact = spark_task("compact_stream_tables", "compact_stream_tables.py", ["--days", "2"])
    reference = spark_task("load_reference", "build_gold.py", ["--mart", "reference"])
    marts = [spark_task(m, "build_gold.py", ["--mart", m, "--days", "2"]) for m in MARTS]
    dq = spark_task("dq_gold", "dq_gold.py", ["--days", "2"], retries=0)

    cross_downstream([compact, reference], marts)
    marts >> dq
