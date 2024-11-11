from airflow.models.dag import DAG
import pendulum

from airflow.operators.bash import (
    BashOperator
)

with DAG(
    dag_id="dag_air_polution",
    schedule=None,
    start_date=pendulum.datetime(2024, 9, 16, tz="UTC"),
    catchup=False
):
    get_data_from_api = BashOperator(
        task_id = 'get_data_from_api',
        bash_command = "python3 ~/airflow/python/get_data_from_api.py"
    )
    
    
get_data_from_api