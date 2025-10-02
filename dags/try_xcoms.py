# xcom.py
import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task, dag


@task(task_id="push")
def _push(ti):
    ti.xcom_push(key="animal", value="cat")
    return "foo"

@task(task_id="pull")
def _pull(ti):
    animal = ti.xcom_pull(task_ids="push", key="animal")
    print(f"This is a {animal}!")

@dag(dag_id="try_xcom", start_date=datetime.datetime(2025, 10, 1), schedule=None)
def xcom():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> _push() >> _pull() >> end

xcom()