from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from helpers.cloud_helper import (
    required_env,
    run_bq_load_from_gcs,
    run_bq_query,
    table_name_only,
)

RAW_EXPORT_DIR = Path("/opt/airflow/project/scripts/cloud_export/access")
GCS_PREFIX = "access/raw"

ACCESS_RAW_TABLES = [
    "raw.access_system_accounts_raw",
    "raw.access_role_assignments_raw",
    "raw.access_login_events_raw",
    "raw.access_privileged_access_raw",
    "raw.access_file_operations_raw",
    "raw.access_network_activity_raw",
]


def check_greenplum_connection() -> None:
    hook = PostgresHook(postgres_conn_id="greenplum_dwh")
    records = hook.get_records("select 1;")
    if not records or records[0][0] != 1:
        raise ValueError(f"Unexpected Greenplum test result: {records}")


def check_gcp_environment() -> None:
    required = [
        "GCP_PROJECT_ID",
        "GCP_REGION",
        "GCS_STAGING_BUCKET",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise EnvironmentError(f"Missing GCP environment variables: {missing}")

    key_path = Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    if not key_path.exists():
        raise FileNotFoundError(f"GCP key file not found: {key_path}")


def export_access_raw_from_greenplum_to_parquet() -> None:
    RAW_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    hook = PostgresHook(postgres_conn_id="greenplum_dwh")
    engine = hook.get_sqlalchemy_engine()

    for source_table in ACCESS_RAW_TABLES:
        table_name = table_name_only(source_table)
        output_file = RAW_EXPORT_DIR / f"{table_name}.parquet"

        df = pd.read_sql(f"select * from {source_table}", con=engine)

        if df.empty:
            print(f"Table {source_table} is empty. Writing empty parquet: {output_file}")
        else:
            print(f"Exporting {source_table}: {len(df)} rows -> {output_file}")

        df.to_parquet(output_file, engine="pyarrow", index=False)


def upload_access_raw_to_gcs() -> None:
    bucket_name = required_env("GCS_STAGING_BUCKET")
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    for source_table in ACCESS_RAW_TABLES:
        table_name = table_name_only(source_table)
        local_file = RAW_EXPORT_DIR / f"{table_name}.parquet"
        object_name = f"{GCS_PREFIX}/{table_name}.parquet"

        if not local_file.exists():
            raise FileNotFoundError(f"Export file not found: {local_file}")

        hook.upload(
            bucket_name=bucket_name,
            object_name=object_name,
            filename=str(local_file),
        )
        print(f"Uploaded {local_file} -> gs://{bucket_name}/{object_name}")


def load_access_raw_to_bigquery() -> None:
    bucket_name = required_env("GCS_STAGING_BUCKET")
    location = required_env("GCP_REGION")

    for source_table in ACCESS_RAW_TABLES:
        table_name = table_name_only(source_table)
        source_uri = f"gs://{bucket_name}/{GCS_PREFIX}/{table_name}.parquet"

        print(f"Start loading {source_uri} -> access_raw.{table_name} ({location})")

        payload = run_bq_load_from_gcs(
            source_uri=source_uri,
            dataset_id="access_raw",
            table_id=table_name,
            location=location,
        )

        print(
            f"Loaded {source_uri} -> access_raw.{table_name}. "
            f"State={payload.get('status', {}).get('state')}"
        )


def validate_access_raw_counts() -> None:
    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")
    pg_hook = PostgresHook(postgres_conn_id="greenplum_dwh")

    for source_table in ACCESS_RAW_TABLES:
        table_name = table_name_only(source_table)

        pg_count = pg_hook.get_first(f"select count(*) from {source_table};")[0]

        query = f"""
        select count(*) as cnt
        from `{project_id}.access_raw.{table_name}`
        """
        result = run_bq_query(query=query, location=location)

        rows = result.get("rows", [])
        if not rows:
            raise RuntimeError(f"No rows returned while validating table {table_name}")

        bq_count = int(rows[0]["f"][0]["v"])

        print(f"Validation for {table_name}: Greenplum={pg_count}, BigQuery={bq_count}")

        if pg_count != bq_count:
            raise ValueError(
                f"Count mismatch for {table_name}: Greenplum={pg_count}, BigQuery={bq_count}"
            )


with DAG(
    dag_id="access_cloud_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "bigquery", "gcs", "access", "migration", "parquet", "raw"],
) as dag:
    check_gp = PythonOperator(
        task_id="check_greenplum_connection",
        python_callable=check_greenplum_connection,
    )

    check_gcp = PythonOperator(
        task_id="check_gcp_environment",
        python_callable=check_gcp_environment,
    )

    export_raw = PythonOperator(
        task_id="export_access_raw_from_greenplum_to_parquet",
        python_callable=export_access_raw_from_greenplum_to_parquet,
    )

    upload_raw = PythonOperator(
        task_id="upload_access_raw_to_gcs",
        python_callable=upload_access_raw_to_gcs,
    )

    load_raw = PythonOperator(
        task_id="load_access_raw_to_bigquery",
        python_callable=load_access_raw_to_bigquery,
    )

    validate_raw = PythonOperator(
        task_id="validate_access_raw_counts",
        python_callable=validate_access_raw_counts,
    )

    check_gp >> check_gcp >> export_raw >> upload_raw >> load_raw >> validate_raw