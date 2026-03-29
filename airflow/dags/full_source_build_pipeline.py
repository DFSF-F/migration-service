from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="full_source_build_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["greenplum", "orchestration", "migration"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_hr = TriggerDagRunOperator(
        task_id="run_hr_pipeline",
        trigger_dag_id="hr_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_risk = TriggerDagRunOperator(
        task_id="run_risk_pipeline",
        trigger_dag_id="risk_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_access = TriggerDagRunOperator(
        task_id="run_access_pipeline",
        trigger_dag_id="access_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_finance = TriggerDagRunOperator(
        task_id="run_finance_pipeline",
        trigger_dag_id="finance_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    finish = EmptyOperator(task_id="finish")

    start >> run_hr >> [run_risk, run_access, run_finance] >> finish