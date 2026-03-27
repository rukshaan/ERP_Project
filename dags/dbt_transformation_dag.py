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
    schedule=None,
    catchup=False,
    tags=['dbt', 'transformations']
) as dag:

    # 1️⃣ Debug dbt
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt debug --profiles-dir /opt/airflow/dbt_pro
        """,
        retries=3,
        retry_delay=timedelta(seconds=30)
    )

    # 2️⃣ Install dbt packages
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt deps --profiles-dir /opt/airflow/dbt_pro
        """
    )

    # 3️⃣ Run dbt snapshots
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt snapshot --profiles-dir /opt/airflow/dbt_pro
        """
    )

    # 4️⃣ Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt run --profiles-dir /opt/airflow/dbt_pro
        """
    )

    # 5️⃣ Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt test --profiles-dir /opt/airflow/dbt_pro
        """
    )

    # 6️⃣ Generate dbt docs
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project &&
            dbt docs generate --profiles-dir /opt/airflow/dbt_pro
        """
    )

    # 7️⃣ Serve docs using Python HTTP server
    dbt_docs_serve = BashOperator(
        task_id='dbt_docs_serve',
        bash_command="""
            cd /opt/airflow/dbt_pro/my_dbt_project/target &&
            python -m http.server 8085
        """,
        # Optional: you can set retries if you want
    )

    # Dependencies
    dbt_debug >> dbt_deps >> dbt_snapshot >> dbt_run >> dbt_test >> dbt_docs_generate >> dbt_docs_serve