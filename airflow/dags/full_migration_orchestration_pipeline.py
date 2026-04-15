from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="full_migration_orchestration_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["migration", "orchestration", "gcp", "bigquery", "gcs"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_access = TriggerDagRunOperator(
        task_id="run_access_cloud_pipeline",
        trigger_dag_id="access_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=["failed"],
        allowed_states=["success"],
    )

    run_finance = TriggerDagRunOperator(
        task_id="run_finance_cloud_pipeline",
        trigger_dag_id="finance_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=["failed"],
        allowed_states=["success"],
    )

    run_hr = TriggerDagRunOperator(
        task_id="run_hr_cloud_pipeline",
        trigger_dag_id="hr_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=["failed"],
        allowed_states=["success"],
    )

    run_risk = TriggerDagRunOperator(
        task_id="run_risk_cloud_pipeline",
        trigger_dag_id="risk_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=["failed"],
        allowed_states=["success"],
    )

    finish = EmptyOperator(task_id="finish")

    start >> [run_access, run_finance, run_hr, run_risk] >> finish