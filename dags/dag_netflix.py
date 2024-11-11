from airflow.models.dag import DAG

from airflow.operators.bash import (
    BashOperator
)

with DAG(
    dag_id="dag_netflix",
    schedule=None,
    catchup=False
):
    clean_netflix_data = BashOperator(
        task_id = 'clean_netflix_dataset',
        bash_command = "python3 ~/airflow/python/clean_netflix_dataset.py"
    )
    
    netflix_dimensions = BashOperator(
        task_id = 'netflix_dimensions',
        bash_command = "python3 ~/airflow/python/netflix_dimensions.py"
    )
    
    
clean_netflix_data > netflix_dimensions