import os
import json
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
import requests
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from tasks.transform_salesorder_to_silver import transform_salesorder_to_silver
from tasks.task2 import transform_customer_to_silver
from tasks.check_delta import check_schema
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 15),
}

with DAG(
    dag_id="sales_order_bronze_silver",
    default_args=default_args,
    schedule="0 * * * *",  # Run hourly
    catchup=False,
) as dag:

    

    silver_task = PythonOperator(
        task_id="transform_salesorder_to_silver",
        python_callable=transform_salesorder_to_silver,
    )
    customer_task = PythonOperator(
        task_id="transform_customer_to_silver",
        python_callable=transform_customer_to_silver,
    )
    trigger_third_dag = TriggerDagRunOperator(
        task_id="trigger_Third_dag",
        trigger_dag_id="dbt_transformation_dag",  # ✅ EXACT dag_id
        reset_dag_run=True,
        wait_for_completion=False,  # recommended
    )
    customer_task >> silver_task >> trigger_third_dag

    # schema_check = PythonOperator(
    #     task_id="check_schema_files",
    #     python_callable=check_schema,
    # )

    