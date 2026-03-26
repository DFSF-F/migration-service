from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello() -> None:
    print("Airflow is working correctly.")


def list_dag_files() -> None:
    import os

    dags_path = "/opt/airflow/dags"
    files = os.listdir(dags_path)
    print(f"DAG files in {dags_path}:")
    for file_name in files:
        print(f"- {file_name}")


with DAG(
    dag_id="test_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "setup"],
) as dag:
    task_print_hello = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    task_list_dag_files = PythonOperator(
        task_id="list_dag_files",
        python_callable=list_dag_files,
    )

    task_print_hello >> task_list_dag_files