from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='generate_data',  
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False
) as dag:

    
    generate_data_task = BashOperator(
        task_id='generate_data_task',
        bash_command='python /home/meqlad/airflow_customers_data_pipeline/generate_data.py',  
    )


    generate_data_task
