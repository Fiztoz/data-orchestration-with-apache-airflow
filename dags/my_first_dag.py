from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
# from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    "my_first_dag",
    start_date=timezone.datetime(2025, 10, 2),
    schedule=None,
):
    tasks = {f"t{i}": EmptyOperator(task_id=f"t{i}") for i in range(1, 10)}
    t1, t2, t3, t4, t5, t6, t7, t8, t9 = (tasks[f"t{i}"] for i in range(1, 10))

    t1 >> [t2, t5]
    t2 >> t3 >> t4 >> t9
    t2 >> t6
    t5 >> [t6, t7]
    [t6, t7] >> t8 >> t9
