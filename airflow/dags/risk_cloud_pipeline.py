from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.auth.transport.requests import Request
from google.oauth2 import service_account


RAW_EXPORT_DIR = Path("/opt/airflow/project/scripts/cloud_export/risk")
BQ_SQL_DIR = Path("/opt/airflow/project/bigquery")
GCS_PREFIX = "risk/raw"

RISK_RAW_TABLES = [
    "raw.risk_org_structure_raw",
    "raw.risk_employee_registry_raw",
    "raw.risk_ib_incidents_raw",
    "raw.risk_security_incidents_raw",
    "raw.risk_compliance_incidents_raw",
    "raw.risk_nonwork_activity_raw",
]


def _table_name_only(full_name: str) -> str:
    return full_name.split(".")[-1]


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def _access_token() -> str:
    key_path = _required_env("GOOGLE_APPLICATION_CREDENTIALS")

    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
    ]

    creds = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=scopes,
    )
    creds = creds.with_always_use_jwt_access(False)
    creds.refresh(Request())

    if not creds.token:
        raise RuntimeError("Failed to obtain GCP access token from service account credentials.")

    return creds.token


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
    json_body: dict | None = None,
    timeout: int = 120,
    max_attempts: int = 5,
    sleep_seconds: int = 3,
) -> dict:
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )

            if response.status_code >= 500:
                if attempt < max_attempts:
                    time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(
                    f"HTTP {response.status_code}\nBODY:\n{response.text[:4000]}"
                )

            if response.status_code >= 400:
                raise RuntimeError(
                    f"HTTP {response.status_code}\nBODY:\n{response.text[:4000]}"
                )

            try:
                return response.json()
            except json.JSONDecodeError as e:
                raise RuntimeError(
                    "Failed to decode JSON response.\n"
                    f"BODY:\n{response.text[:4000]}"
                ) from e

        except requests.exceptions.RequestException as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep(sleep_seconds)
                continue
            raise RuntimeError(
                "HTTP request to BigQuery failed after retries.\n"
                f"Error: {repr(e)}"
            ) from e

    raise RuntimeError(f"Unexpected request failure: {repr(last_error)}")


def _submit_bq_job(configuration: dict, location: str | None = None) -> dict:
    project_id = _required_env("GCP_PROJECT_ID")
    token = _access_token()

    payload = {"configuration": configuration}
    if location:
        payload["jobReference"] = {
            "projectId": project_id,
            "location": location,
        }

    response_json = _request_json(
        "POST",
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs",
        headers={
            "Authorization": f"Bearer {token}",
            "X-Goog-User-Project": project_id,
            "Content-Type": "application/json",
        },
        json_body=payload,
    )

    if "error" in response_json:
        raise RuntimeError(
            f"BigQuery jobs.insert returned error: {json.dumps(response_json, ensure_ascii=False)}"
        )

    return response_json


def _wait_bq_query_job(job_id: str, location: str) -> dict:
    project_id = _required_env("GCP_PROJECT_ID")

    for _ in range(120):
        token = _access_token()

        payload = _request_json(
            "GET",
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries/{job_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Goog-User-Project": project_id,
            },
            params={"location": location},
        )

        if "error" in payload:
            raise RuntimeError(
                f"BigQuery getQueryResults returned error: {json.dumps(payload, ensure_ascii=False)}"
            )

        if payload.get("jobComplete") is True:
            return payload

        time.sleep(2)

    raise TimeoutError(f"Timed out waiting for BigQuery query job {job_id}.")


def _wait_bq_load_job(job_id: str, location: str) -> dict:
    project_id = _required_env("GCP_PROJECT_ID")

    for _ in range(120):
        token = _access_token()

        payload = _request_json(
            "GET",
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs/{job_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Goog-User-Project": project_id,
            },
            params={"location": location},
        )

        state = payload.get("status", {}).get("state")
        if state == "DONE":
            errors = payload.get("status", {}).get("errors")
            error_result = payload.get("status", {}).get("errorResult")
            if errors or error_result:
                raise RuntimeError(
                    f"BigQuery load job failed: {json.dumps(payload.get('status', {}), ensure_ascii=False)}"
                )
            return payload

        time.sleep(2)

    raise TimeoutError(f"Timed out waiting for BigQuery load job {job_id}.")


def _run_bq_query(query: str, location: str) -> dict:
    job = _submit_bq_job(
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
        location=location,
    )

    job_ref = job["jobReference"]
    return _wait_bq_query_job(job_ref["jobId"], job_ref["location"])


def _run_bq_load_from_gcs(
    source_uri: str,
    dataset_id: str,
    table_id: str,
    location: str,
) -> dict:
    project_id = _required_env("GCP_PROJECT_ID")

    job = _submit_bq_job(
        configuration={
            "load": {
                "sourceUris": [source_uri],
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "autodetect": True,
                "ignoreUnknownValues": False,
            }
        },
        location=location,
    )

    job_ref = job["jobReference"]
    return _wait_bq_load_job(job_ref["jobId"], job_ref["location"])


def _read_sql_file(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    return file_path.read_text(encoding="utf-8")


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


def export_risk_raw_from_greenplum_to_parquet() -> None:
    RAW_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    hook = PostgresHook(postgres_conn_id="greenplum_dwh")
    engine = hook.get_sqlalchemy_engine()

    for source_table in RISK_RAW_TABLES:
        table_name = _table_name_only(source_table)
        output_file = RAW_EXPORT_DIR / f"{table_name}.parquet"

        query = f"select * from {source_table}"
        df = pd.read_sql(query, con=engine)

        if df.empty:
            print(f"Table {source_table} is empty. Writing empty parquet: {output_file}")
        else:
            print(f"Exporting {source_table}: {len(df)} rows -> {output_file}")

        df.to_parquet(output_file, engine="pyarrow", index=False)


def upload_risk_raw_to_gcs() -> None:
    bucket_name = _required_env("GCS_STAGING_BUCKET")
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    for source_table in RISK_RAW_TABLES:
        table_name = _table_name_only(source_table)
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


def check_bigquery_access() -> None:
    location = _required_env("GCP_REGION")
    result = _run_bq_query("select 1 as x", location=location)
    print(f"BigQuery access OK. jobComplete={result.get('jobComplete')}")


def load_risk_raw_to_bigquery() -> None:
    bucket_name = _required_env("GCS_STAGING_BUCKET")
    location = _required_env("GCP_REGION")

    for source_table in RISK_RAW_TABLES:
        table_name = _table_name_only(source_table)
        source_uri = f"gs://{bucket_name}/{GCS_PREFIX}/{table_name}.parquet"

        print(f"Start loading {source_uri} -> risk_raw.{table_name} ({location})")

        payload = _run_bq_load_from_gcs(
            source_uri=source_uri,
            dataset_id="risk_raw",
            table_id=table_name,
            location=location,
        )

        print(
            f"Loaded {source_uri} -> risk_raw.{table_name}. "
            f"State={payload.get('status', {}).get('state')}"
        )


def validate_risk_raw_counts() -> None:
    project_id = _required_env("GCP_PROJECT_ID")
    location = _required_env("GCP_REGION")
    pg_hook = PostgresHook(postgres_conn_id="greenplum_dwh")

    for source_table in RISK_RAW_TABLES:
        table_name = _table_name_only(source_table)

        pg_count = pg_hook.get_first(f"select count(*) from {source_table};")[0]

        query = f"""
        select count(*) as cnt
        from `{project_id}.risk_raw.{table_name}`
        """
        result = _run_bq_query(query=query, location=location)

        rows = result.get("rows", [])
        if not rows:
            raise RuntimeError(f"No rows returned while validating table {table_name}")

        bq_count = int(rows[0]["f"][0]["v"])

        print(f"Validation for {table_name}: Greenplum={pg_count}, BigQuery={bq_count}")

        if pg_count != bq_count:
            raise ValueError(
                f"Count mismatch for {table_name}: Greenplum={pg_count}, BigQuery={bq_count}"
            )


def build_risk_dds() -> None:
    location = _required_env("GCP_REGION")
    project_id = _required_env("GCP_PROJECT_ID")

    sql = _read_sql_file(BQ_SQL_DIR / "risk_dds.sql").replace("{{PROJECT_ID}}", project_id)
    result = _run_bq_query(sql, location=location)

    print(f"risk_dds.sql executed. jobComplete={result.get('jobComplete')}")


def build_risk_dm() -> None:
    location = _required_env("GCP_REGION")
    project_id = _required_env("GCP_PROJECT_ID")

    sql = _read_sql_file(BQ_SQL_DIR / "risk_dm.sql").replace("{{PROJECT_ID}}", project_id)
    result = _run_bq_query(sql, location=location)

    print(f"risk_dm.sql executed. jobComplete={result.get('jobComplete')}")


with DAG(
    dag_id="risk_cloud_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "bigquery", "gcs", "risk", "migration", "parquet"],
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
        task_id="export_risk_raw_from_greenplum_to_parquet",
        python_callable=export_risk_raw_from_greenplum_to_parquet,
    )

    upload_raw = PythonOperator(
        task_id="upload_risk_raw_to_gcs",
        python_callable=upload_risk_raw_to_gcs,
    )

    check_bq = PythonOperator(
        task_id="check_bigquery_access",
        python_callable=check_bigquery_access,
    )

    load_raw = PythonOperator(
        task_id="load_risk_raw_to_bigquery",
        python_callable=load_risk_raw_to_bigquery,
    )

    validate_raw = PythonOperator(
        task_id="validate_risk_raw_counts",
        python_callable=validate_risk_raw_counts,
    )

    build_dds = PythonOperator(
        task_id="build_risk_dds",
        python_callable=build_risk_dds,
    )

    build_dm = PythonOperator(
        task_id="build_risk_dm",
        python_callable=build_risk_dm,
    )

    check_gp >> check_gcp >> export_raw >> upload_raw >> check_bq >> load_raw >> validate_raw >> build_dds >> build_dm