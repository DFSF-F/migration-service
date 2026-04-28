from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def table_name_only(full_name: str) -> str:
    return full_name.split(".")[-1]


def read_sql_file(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    return file_path.read_text(encoding="utf-8")


def get_gcloud_access_token() -> str:
    key_path = Path(required_env("GOOGLE_APPLICATION_CREDENTIALS"))
    if not key_path.exists():
        raise FileNotFoundError(f"GCP key file not found: {key_path}")

    scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(key_path),
            scopes=scopes,
        )
        credentials.refresh(Request())
    except Exception as e:
        raise RuntimeError(f"Failed to obtain GCP access token from service account key: {e}") from e

    token = credentials.token
    if not token:
        raise RuntimeError("Access token is empty after credentials refresh.")

    return token


def submit_bq_job(configuration: dict, location: str | None = None) -> dict:
    project_id = required_env("GCP_PROJECT_ID")
    token = get_gcloud_access_token()

    payload = {"configuration": configuration}
    if location:
        payload["jobReference"] = {
            "projectId": project_id,
            "location": location,
        }

    response = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs",
        headers={
            "Authorization": f"Bearer {token}",
            "X-Goog-User-Project": project_id,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )

    if response.status_code >= 400:
        raise RuntimeError(
            "BigQuery jobs.insert failed.\n"
            f"HTTP {response.status_code}\n"
            f"BODY:\n{response.text[:4000]}"
        )

    try:
        response_json = response.json()
    except json.JSONDecodeError as e:
        raise RuntimeError(
            "Failed to decode BigQuery jobs.insert response.\n"
            f"BODY:\n{response.text[:4000]}"
        ) from e

    if "error" in response_json:
        raise RuntimeError(
            f"BigQuery jobs.insert returned error: {json.dumps(response_json, ensure_ascii=False)}"
        )

    return response_json


def wait_bq_query_job(job_id: str, location: str, max_polls: int = 120, sleep_sec: int = 2) -> dict:
    project_id = required_env("GCP_PROJECT_ID")
    token = get_gcloud_access_token()

    for _ in range(max_polls):
        response = requests.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries/{job_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Goog-User-Project": project_id,
            },
            params={"location": location},
            timeout=120,
        )

        if response.status_code >= 400:
            raise RuntimeError(
                "BigQuery getQueryResults failed.\n"
                f"HTTP {response.status_code}\n"
                f"BODY:\n{response.text[:4000]}"
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(
                "Failed to decode BigQuery getQueryResults response.\n"
                f"BODY:\n{response.text[:4000]}"
            ) from e

        if "error" in payload:
            raise RuntimeError(
                f"BigQuery getQueryResults returned error: {json.dumps(payload, ensure_ascii=False)}"
            )

        if payload.get("jobComplete") is True:
            return payload

        time.sleep(sleep_sec)

    raise TimeoutError(f"Timed out waiting for BigQuery query job {job_id}.")


def wait_bq_load_job(job_id: str, location: str, max_polls: int = 120, sleep_sec: int = 2) -> dict:
    project_id = required_env("GCP_PROJECT_ID")
    token = get_gcloud_access_token()

    for _ in range(max_polls):
        response = requests.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs/{job_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Goog-User-Project": project_id,
            },
            params={"location": location},
            timeout=120,
        )

        if response.status_code >= 400:
            raise RuntimeError(
                "BigQuery jobs.get failed.\n"
                f"HTTP {response.status_code}\n"
                f"BODY:\n{response.text[:4000]}"
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(
                "Failed to decode BigQuery jobs.get response.\n"
                f"BODY:\n{response.text[:4000]}"
            ) from e

        state = payload.get("status", {}).get("state")
        if state == "DONE":
            errors = payload.get("status", {}).get("errors")
            error_result = payload.get("status", {}).get("errorResult")
            if errors or error_result:
                raise RuntimeError(
                    f"BigQuery load job failed: {json.dumps(payload.get('status', {}), ensure_ascii=False)}"
                )
            return payload

        time.sleep(sleep_sec)

    raise TimeoutError(f"Timed out waiting for BigQuery load job {job_id}.")


def run_bq_query(query: str, location: str) -> dict:
    job = submit_bq_job(
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
        location=location,
    )

    job_ref = job["jobReference"]
    return wait_bq_query_job(job_ref["jobId"], job_ref["location"])


def run_bq_load_from_gcs(
    source_uri: str,
    dataset_id: str,
    table_id: str,
    location: str,
) -> dict:
    project_id = required_env("GCP_PROJECT_ID")

    job = submit_bq_job(
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
    return wait_bq_load_job(job_ref["jobId"], job_ref["location"])