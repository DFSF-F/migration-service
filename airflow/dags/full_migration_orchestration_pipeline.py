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
    max_active_runs=1,
    tags=["orchestration", "full-migration"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_full_source_build_pipeline = TriggerDagRunOperator(
        task_id="run_full_source_build_pipeline",
        trigger_dag_id="full_source_build_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_hr_cloud_pipeline = TriggerDagRunOperator(
        task_id="run_hr_cloud_pipeline",
        trigger_dag_id="hr_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_risk_cloud_pipeline = TriggerDagRunOperator(
        task_id="run_risk_cloud_pipeline",
        trigger_dag_id="risk_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_access_cloud_pipeline = TriggerDagRunOperator(
        task_id="run_access_cloud_pipeline",
        trigger_dag_id="access_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_finance_cloud_pipeline = TriggerDagRunOperator(
        task_id="run_finance_cloud_pipeline",
        trigger_dag_id="finance_cloud_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_hr_dbt_pipeline = TriggerDagRunOperator(
        task_id="run_hr_dbt_pipeline",
        trigger_dag_id="hr_dbt_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_risk_dbt_pipeline = TriggerDagRunOperator(
        task_id="run_risk_dbt_pipeline",
        trigger_dag_id="risk_dbt_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_access_dbt_pipeline = TriggerDagRunOperator(
        task_id="run_access_dbt_pipeline",
        trigger_dag_id="access_dbt_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_finance_dbt_pipeline = TriggerDagRunOperator(
        task_id="run_finance_dbt_pipeline",
        trigger_dag_id="finance_dbt_pipeline",
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    finish = EmptyOperator(task_id="finish")

    start >> run_full_source_build_pipeline

    run_full_source_build_pipeline >> [
        run_hr_cloud_pipeline,
        run_risk_cloud_pipeline,
        run_access_cloud_pipeline,
        run_finance_cloud_pipeline,
    ]

    run_hr_cloud_pipeline >> run_hr_dbt_pipeline

    run_hr_dbt_pipeline >> [
        run_risk_dbt_pipeline,
        run_access_dbt_pipeline,
        run_finance_dbt_pipeline,
    ]

    run_risk_cloud_pipeline >> run_risk_dbt_pipeline
    run_access_cloud_pipeline >> run_access_dbt_pipeline
    run_finance_cloud_pipeline >> run_finance_dbt_pipeline

    [
        run_risk_dbt_pipeline,
        run_access_dbt_pipeline,
        run_finance_dbt_pipeline,
    ] >> finish