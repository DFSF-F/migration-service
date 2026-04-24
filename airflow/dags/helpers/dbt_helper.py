from __future__ import annotations

import os

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


def get_dbt_project_dir() -> str:
    return os.environ.get("DBT_PROJECT_DIR", "/app/dbt")


def get_dbt_image() -> str:
    return os.environ.get("DBT_IMAGE_NAME", "migration-service-dbt-runner")


def get_docker_network() -> str:
    return os.environ.get("AIRFLOW_DOCKER_NETWORK", "migration-service_default")


def get_host_project_root() -> str:
    value = os.environ.get("HOST_PROJECT_ROOT")
    if not value:
        raise EnvironmentError("Missing required environment variable: HOST_PROJECT_ROOT")
    return value


def get_dbt_mounts() -> list[Mount]:
    host_project_root = get_host_project_root()

    return [
        Mount(
            source=f"{host_project_root}/dbt",
            target="/app/dbt",
            type="bind",
        ),
        Mount(
            source=f"{host_project_root}/secrets",
            target="/app/secrets",
            type="bind",
            read_only=True,
        ),
    ]


def get_dbt_env() -> dict[str, str]:
    required_vars = ["GCP_PROJECT_ID", "GCP_REGION"]
    missing = [name for name in required_vars if not os.environ.get(name)]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables for dbt: {missing}"
        )

    return {
        "GCP_PROJECT_ID": os.environ["GCP_PROJECT_ID"],
        "GCP_REGION": os.environ["GCP_REGION"],
    }


def build_dbt_debug_task(task_id: str = "dbt_debug") -> DockerOperator:
    dbt_project_dir = get_dbt_project_dir()

    return DockerOperator(
        task_id=task_id,
        image=get_dbt_image(),
        api_version="auto",
        auto_remove="success",
        command=f"bash -c 'cd {dbt_project_dir} && dbt debug'",
        docker_url="unix://var/run/docker.sock",
        network_mode=get_docker_network(),
        mount_tmp_dir=False,
        mounts=get_dbt_mounts(),
        environment=get_dbt_env(),
    )


def build_dbt_run_task(domain: str, task_id: str | None = None) -> DockerOperator:
    dbt_project_dir = get_dbt_project_dir()
    resolved_task_id = task_id or f"dbt_run_{domain}"

    return DockerOperator(
        task_id=resolved_task_id,
        image=get_dbt_image(),
        api_version="auto",
        auto_remove="success",
        command=(
            f"bash -c 'cd {dbt_project_dir} "
            f"&& dbt clean "
            f"&& dbt run --select path:models/{domain} --threads 1 --no-partial-parse'"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode=get_docker_network(),
        mount_tmp_dir=False,
        mounts=get_dbt_mounts(),
        environment=get_dbt_env(),
    )