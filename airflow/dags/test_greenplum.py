from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="test_greenplum_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_connection = PostgresOperator(
        task_id="test_connection",
        postgres_conn_id="greenplum_dwh",
        sql='''
        create table if not exists airflow_test (id int);
        insert into airflow_test values (1);
        ''',
    )