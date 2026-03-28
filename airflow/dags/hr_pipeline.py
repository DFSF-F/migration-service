from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook


SQL_ROOT = Path("/opt/airflow/project/sql/hr")
RAW_OUTPUT_DIR = Path("/opt/airflow/project/scripts/data_gen/output")


def execute_sql_file(filename: str) -> None:
    file_path = SQL_ROOT / filename
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    sql_text = file_path.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    hook = PostgresHook(postgres_conn_id="greenplum_dwh")
    conn = hook.get_conn()

    try:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
        conn.commit()
    finally:
        conn.close()


def load_raw_to_greenplum() -> None:
    file_mapping = [
        ("raw.hr_employee_master_raw", "hr_employee_master_raw.csv"),
        ("raw.hr_position_history_raw", "hr_position_history_raw.csv"),
        ("raw.hr_department_history_raw", "hr_department_history_raw.csv"),
        ("raw.hr_absence_events_raw", "hr_absence_events_raw.csv"),
        ("raw.hr_overtime_events_raw", "hr_overtime_events_raw.csv"),
        ("raw.hr_dismissal_signals_raw", "hr_dismissal_signals_raw.csv"),
    ]

    truncate_sql = """
    truncate table raw.hr_employee_master_raw;
    truncate table raw.hr_position_history_raw;
    truncate table raw.hr_department_history_raw;
    truncate table raw.hr_absence_events_raw;
    truncate table raw.hr_overtime_events_raw;
    truncate table raw.hr_dismissal_signals_raw;
    """

    hook = PostgresHook(postgres_conn_id="greenplum_dwh")
    conn = hook.get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute(truncate_sql)

            for table_name, file_name in file_mapping:
                file_path = RAW_OUTPUT_DIR / file_name
                if not file_path.exists():
                    raise FileNotFoundError(f"Raw file not found: {file_path}")

                with file_path.open("r", encoding="utf-8") as f:
                    cur.copy_expert(
                        f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
                        f,
                    )

        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="hr_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["greenplum", "hr", "migration"],
) as dag:

    check_connection = SQLExecuteQueryOperator(
        task_id="check_connection",
        conn_id="greenplum_dwh",
        sql="select 1;",
    )

    create_raw_tables = PythonOperator(
        task_id="create_raw_tables",
        python_callable=execute_sql_file,
        op_kwargs={"filename": "001_raw_tables.sql"},
    )

    create_dds_tables = PythonOperator(
        task_id="create_dds_tables",
        python_callable=execute_sql_file,
        op_kwargs={"filename": "002_dds_tables.sql"},
    )

    create_dm_objects = PythonOperator(
        task_id="create_dm_objects",
        python_callable=execute_sql_file,
        op_kwargs={"filename": "003_dm_objects.sql"},
    )

    generate_raw_files = BashOperator(
        task_id="generate_raw_files",
        bash_command=(
            "python3 /opt/airflow/project/scripts/data_gen/generate_hr_raw_data.py "
            "--output-dir /opt/airflow/project/scripts/data_gen/output"
        ),
    )

    load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_to_greenplum,
    )

    build_dds = PythonOperator(
        task_id="build_dds",
        python_callable=execute_sql_file,
        op_kwargs={"filename": "005_build_dds.sql"},
    )

    build_dm = PythonOperator(
        task_id="build_dm",
        python_callable=execute_sql_file,
        op_kwargs={"filename": "006_build_dm.sql"},
    )

    check_dm_not_empty = SQLCheckOperator(
        task_id="check_dm_not_empty",
        conn_id="greenplum_dwh",
        sql="select count(*) > 0 from dm.employee_hr_profile_report;",
    )

    (
        check_connection
        >> create_raw_tables
        >> create_dds_tables
        >> create_dm_objects
        >> generate_raw_files
        >> load_raw_data
        >> build_dds
        >> build_dm
        >> check_dm_not_empty
    )