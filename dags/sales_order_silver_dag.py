import os
import json
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
import requests
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from tasks.transform_salesorder_to_silver import transform_salesorder_to_silver


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 15),
}

with DAG(
    dag_id="sales_order_bronze_silver",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    

    silver_task = PythonOperator(
        task_id="transform_salesorder_to_silver",
        python_callable=transform_salesorder_to_silver,
    )

    silver_task
