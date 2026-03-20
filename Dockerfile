FROM apache/airflow:latest

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk wget unzip
USER airflow

# ---- INSTALL PYTHON PACKAGES ----
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.0 \
    duckdb \
    dbt-core \
    dbt-duckdb \
    python-dotenv \
    pandas \
    pyarrow \
    deepdiff \
    deltalake \
    great-expectations==0.15.13 \
    streamlit

# ---- INSTALL MATCHING DELTA LAKE JARS ----
USER root
RUN mkdir -p /opt/spark/jars && \
    echo "Downloading Delta JARs..." && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
         -O /opt/spark/jars/delta-spark.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
         -O /opt/spark/jars/delta-storage.jar && \
    wget -q https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar \
         -O /opt/spark/jars/antlr4-runtime.jar && \
    echo "Downloaded JARs:" && ls -la /opt/spark/jars/ && \
    chmod 644 /opt/spark/jars/*.jar
USER airflow
