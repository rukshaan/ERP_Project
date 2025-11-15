import os
import json
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
import requests
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from tasks.task1 import say_hello
from tasks.task2 import say_hello_2

import json
 
 
 
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 15),
    'execution_timeout': timedelta(minutes=30),
}
 
# Define the DAG
with DAG(
    'first_dag',
    default_args=default_args,
    description='Fetch user data from API and store it in DuckDB',
    schedule='@daily', 
    catchup=False,
    
) as dag:
 

# check schema
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=say_hello,
        
        dag=dag,
    )

    # task_2 = PythonOperator(
    #     task_id='task_2',
    #     python_callable=say_hello_2,
    # )
 
 

 
    # task_2 >> task_1
 
 
 