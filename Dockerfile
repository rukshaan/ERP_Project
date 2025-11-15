FROM apache/airflow:latest

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk
USER airflow

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
