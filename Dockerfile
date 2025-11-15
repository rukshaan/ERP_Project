FROM apache/airflow:latest

# Switch to root to install system packages
USER root

# Install Java
RUN apt-get update && apt-get install -y openjdk-17-jdk

# Switch back to airflow user for Python installations
USER airflow

# Install all Python dependencies (packages you had in _PIP_ADDITIONAL_REQUIREMENTS)
RUN pip install \
    duckdb \
    dbt-core \
    dbt-duckdb \
    python-dotenv \
    pandas \
    pyarrow \
    deepdiff \
    delta-spark \
    pyspark \
    deltalake \
    great-expectations==0.15.13 \
    streamlit
