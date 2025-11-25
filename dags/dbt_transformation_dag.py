# dags/dbt_transformation_dag.py
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'erp_project',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Add retry delay
}

with DAG('dbt_transformation_dag', 
         default_args=default_args,
         schedule='@daily',
         catchup=False,
         tags=['dbt', 'transformations']) as dag:
    
    # run_dbt_task_test = BashOperator(
    #     task_id='run_dbt_transform_test',
    #     bash_command="""
    #         cd /opt/airflow/dbt_pro/ && \
    #         echo "1" | dbt init my_dbt_project --profiles-dir /opt/airflow/dbt_pro
    #     """,
    # )

    run_dbt_staging = BashOperator(
        task_id='run_dbt_staging',
        bash_command='cd /opt/airflow/dbt_pro/my_dbt_project && dbt run --profiles-dir /opt/airflow/dbt_pro ',
        # env={'DBT_FULL_REFRESH': '{{ dag_run.conf.get("full_refresh", false) }}'}  # Allow manual full refresh
    )

    # run_dbt_tests = BashOperator(
    #     task_id='run_dbt_tests',
    #     bash_command='cd /opt/airflow/dbt_pro && dbt test'
    # )

    # run_dbt_marts = BashOperator(
    #     task_id='run_dbt_marts',
    #     bash_command='cd /opt/airflow/dbt_pro && dbt run --models marts'
    # )

    # run_dbt_staging 