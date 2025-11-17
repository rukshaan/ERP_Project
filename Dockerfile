FROM apache/airflow:latest

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk wget unzip
USER airflow

# Install Python libs (correct versions!)

# ---- INSTALL MATCHING DELTA LAKE JARS ----
USER root
RUN mkdir -p /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
         -O /opt/spark/jars/delta-spark.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
         -O /opt/spark/jars/delta-storage.jar
USER airflow
