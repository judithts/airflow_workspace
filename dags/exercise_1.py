from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="rocket_launch",
    start_date=datetime(year=2024, month=1, day=1),
    end_date=datetime(year=2024, month=1, day=5),
    schedule="@daily",
):
    procure_rocket_material = EmptyOperator(task_id="procurement_of_material")
    procure_fuel = EmptyOperator(task_id="procurement_of_fuel")
    # build_stage_1 = EmptyOperator(task_id="build_stage_1")
    # build_stage_2 = EmptyOperator(task_id="build_stage_2")
    # build_stage_3 = EmptyOperator(task_id="build_stage_3")
    build_tasks = [EmptyOperator(task_id=f"build_stage_{i+1}") for i in range(3)]
    launch = PythonOperator(task_id="launch", python_callable=lambda: print("Launch!"))   

    #procure_rocket_material >> [build_stage_1, build_stage_2, build_stage_3] >> launch
    procure_rocket_material >> build_tasks >> launch
    procure_fuel >> build_tasks[2]
