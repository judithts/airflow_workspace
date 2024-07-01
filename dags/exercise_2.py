from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="rocket_launch_scheduled",
    start_date=datetime(year=2024, month=3, day=5),
    schedule=timedelta(days=3), # every 3 days
  
):
    procure_rocket_material = EmptyOperator(task_id="procurement_of_material")
    procure_fuel = EmptyOperator(task_id="procurement_of_fuel")
    build_tasks = [EmptyOperator(task_id=f"build_stage_{i+1}") for i in range(3)]
    launch = PythonOperator(task_id="launch", python_callable=lambda: print("Launch!"))   

    procure_rocket_material >> build_tasks >> launch
    procure_fuel >> build_tasks[2]
