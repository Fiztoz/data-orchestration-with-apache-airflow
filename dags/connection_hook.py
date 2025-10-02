# connection_and_hook.py
import datetime
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

def _test_connection():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    hook.test_connection()

def _list_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    tables = hook.get_records(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    )
    logging.info(tables)
    return tables

def _list_customers():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    customers = hook.get_records("SELECT * FROM customers;")
    logging.info(customers)
    return customers

def _list_customers_columns():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    df = hook.get_df("SELECT * FROM customers;")
    logging.info(df.columns.tolist())
    return df.columns.tolist()

with DAG(
    dag_id="connection_and_hook",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    start = EmptyOperator(task_id="start")

    list_tables = PythonOperator(
        task_id="list_tables",
        python_callable=_list_tables,
    )

    list_customers = PythonOperator(
        task_id="list_customers",
        python_callable=_list_customers,
    )

    list_customers_colums = PythonOperator(
        task_id="list_customers_colums",
        python_callable=_list_customers_columns,
    )

    test_connection = PythonOperator(
        task_id="test_connection",
        python_callable=_test_connection,
    )

    end = EmptyOperator(task_id="end")

    start >> test_connection >> [list_tables, list_customers, list_customers_colums] >> end