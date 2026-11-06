"""catalog_daily: build the AI asset risk catalog every night.

  init_tables ─ ingest_packages ─ ingest_osv ─ resolve_entities ─ score_assets ─ publish_risk_updates ─ dq_catalog

Packages are the curated seed list plus the packages the curated MCP servers declare; OSV checks the
versions ingest_packages found, hence the ordering. resolve_entities runs even if an ingest failed
(trigger_rule=all_done): bronze keeps the last good data for that source, so one flaky API never blocks
the catalog; dq_catalog flags stale sources.
"""
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule

JOBS = "/opt/tracewell/spark-jobs/catalog"


def spark_task(task_id: str, script: str, args: list[str] | None = None, **kwargs) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=f"{JOBS}/{script}",
        application_args=args or [],
        py_files=f"{JOBS}/common.py,{JOBS}/transforms.py",
        name=f"tracewell-catalog-{task_id}",
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.cores.max": "1",  # lets two catalog tasks share the 2-core worker
        },
        **kwargs,
    )


with DAG(
    dag_id="catalog_daily",
    description="Ingest package and advisory sources, resolve entities, score with the Tracewell risk model, merge SCD2, publish",
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(hours=1),
    },
    tags=["phase1", "catalog"],
):
    init = spark_task("init_tables", "init_tables.py")
    pkgs = spark_task("ingest_packages", "ingest_packages.py")
    osv = spark_task("ingest_osv", "ingest_osv.py")
    resolve = spark_task("resolve_entities", "resolve_entities.py", trigger_rule=TriggerRule.ALL_DONE)
    score = spark_task("score_assets", "score_assets.py")
    publish = spark_task("publish_risk_updates", "publish_risk_updates.py")
    dq = spark_task("dq_catalog", "dq_catalog.py", retries=0)

    init >> pkgs >> osv >> resolve >> score >> publish >> dq
