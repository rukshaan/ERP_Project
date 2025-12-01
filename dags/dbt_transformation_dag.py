# dags/dbt_transformation_dag.py
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'erp_project',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_transformation_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['dbt', 'transformations']
) as dag:

    # 1️⃣ Install dbt packages (dbt_utils, etc.)
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt deps --profiles-dir /opt/airflow/dbt_pro
        """
    )

    # 2️⃣ Run dbt transformations
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt run --profiles-dir /opt/airflow/dbt_pro
        """
    )

    # 3️⃣ (Optional) Run dbt tests
    # dbt_test = BashOperator(
    #     task_id='dbt_test',
    #     bash_command="""
    #         cd /opt/airflow/dbt_pro/my_dbt_project &&
    #         dbt test --profiles-dir /opt/airflow/dbt_pro
    #     """
    # )

    # Dependencies
    dbt_deps >> dbt_run
    # dbt_run >> dbt_test
