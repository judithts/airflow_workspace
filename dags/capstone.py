from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="rocket_launch_with_context",
    start_date=datetime(year=2024, month=5, day=1),
    schedule=timedelta(days=3), # every 3 days
):
    #task1

    task1 >> task2