from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pprint import pprint

def print_context(**context):
    pprint(f"start date of the interval is {context['execution_date']}")
    pprint(f"run date of the interval is {datetime.now()}")

with DAG(
    dag_id="rocket_launch_with_context",
    start_date=datetime(year=2024, month=5, day=1),
    schedule=timedelta(days=3), # every 3 days
):
    task1 = BashOperator(task_id="task1", bash_command="echo '{{task.task_id}} is running in the {{dag.dag_id}} pipeline'")

    task2 = PythonOperator(task_id="task2", python_callable=print_context)

    task1 >> task2