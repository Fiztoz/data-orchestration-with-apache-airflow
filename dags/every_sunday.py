from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

def _hello():
    print("Hello")

with DAG(
    "every_sunday",
    start_date=timezone.datetime(2025, 10, 2),
    schedule="0 0 * * SUN",
):
    hello = PythonOperator(
        task_id="hello",
        python_callable=_hello,
    )

    weekend = BashOperator(
        task_id="weekend",
        bash_command="echo 'Weekend!'",
    )

    hello >> weekend