import os
import json
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
import requests
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from tasks.task1 import get_sales_data_bronze_dag


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
        python_callable=get_sales_data_bronze_dag,
        
        dag=dag,
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_second_dag',
        trigger_dag_id='sales_order_bronze_silver',
        
        reset_dag_run=True
    )
    
    # # Add this task to your sales_order_bronze_silver DAG
    # trigger_third_dag = TriggerDagRunOperator(
    #     task_id='trigger_dbt_transformation_dag',
    #     trigger_dag_id='dbt_transformation_dag',
    #     reset_dag_run=True
    # )

    # Make sure this is at the end of your task dependencies
    # For example: previous_task >> trigger_third_dag
        # task_2 = PythonOperator(
        #     task_id='task_2',
        #     python_callable=say_hello_2,
        # )
 
 

 
    task_1 >> trigger_second_dag
 
 
 