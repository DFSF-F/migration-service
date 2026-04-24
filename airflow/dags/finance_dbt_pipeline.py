from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DBT_PROJECT_DIR = "/app/dbt"
DBT_IMAGE = "migration-service-dbt-runner"

DBT_MOUNTS = [
    Mount(
        source="/Users/kaemzy/Desktop/migration-service/dbt",
        target="/app/dbt",
        type="bind",
    ),
    Mount(
        source="/Users/kaemzy/Desktop/migration-service/secrets",
        target="/app/secrets",
        type="bind",
        read_only=True,
    ),
]

DBT_ENV = {
    "GCP_PROJECT_ID": "dwh-migration-492618",
    "GCP_REGION": "europe-west1",
}


with DAG(
    dag_id="finance_dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["dbt", "finance", "bigquery"],
) as dag:

    dbt_debug = DockerOperator(
        task_id="dbt_debug",
        image=DBT_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=f"bash -c 'cd {DBT_PROJECT_DIR} && dbt debug'",
        docker_url="unix://var/run/docker.sock",
        network_mode="migration-service_default",
        mount_tmp_dir=False,
        mounts=DBT_MOUNTS,
        environment=DBT_ENV,
    )

    dbt_run_finance = DockerOperator(
        task_id="dbt_run_finance",
        image=DBT_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=f"bash -c 'cd {DBT_PROJECT_DIR} && dbt clean && dbt run --select path:models/finance --threads 1 --no-partial-parse'",
        docker_url="unix://var/run/docker.sock",
        network_mode="migration-service_default",
        mount_tmp_dir=False,
        mounts=DBT_MOUNTS,
        environment=DBT_ENV,
    )

    dbt_debug >> dbt_run_finance