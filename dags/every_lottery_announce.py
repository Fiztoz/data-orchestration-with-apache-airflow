from airflow.sdk import DAG, timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

def _hello():
    print("Hello")

with DAG(
    "every_lottery_announce",
    start_date=timezone.datetime(2025, 10, 2),
    schedule="30 17 1,16 * *",
):
    hello = PythonOperator(
        task_id="hello",
        python_callable=_hello,
    )

    lottery = BashOperator(
        task_id="lottery",
        bash_command="echo 'my Lottery!'",
    )

    hello >> lottery