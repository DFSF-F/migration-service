from __future__ import annotations

from datetime import datetime

from airflow import DAG
from helpers.dbt_helper import build_dbt_debug_task, build_dbt_run_task


with DAG(
    dag_id="risk_dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["dbt", "risk", "bigquery"],
) as dag:
    dbt_debug = build_dbt_debug_task()
    dbt_run_risk = build_dbt_run_task(domain="risk", task_id="dbt_run_risk")

    dbt_debug >> dbt_run_risk