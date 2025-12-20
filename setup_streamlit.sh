#!/bin/bash

echo "Creating Streamlit structure..."

# Create the folder
mkdir -p streamlit

# Create app.py
cat <<EOF > streamlit/app.py
import streamlit as st
import duckdb

DB_PATH = "/opt/airflow/data/Silver/dev.duckdb"

st.set_page_config(page_title="ERP Dashboard", layout="wide")

con = duckdb.connect(DB_PATH, read_only=True)

df = con.execute("""
    SELECT *
    FROM fact_final_joined_files
    LIMIT 100
""").df()

st.title("ERP Dashboard")
st.dataframe(df)
EOF

# Create Dockerfile
cat <<EOF > streamlit/Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN pip install streamlit duckdb pandas

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
EOF

echo " Streamlit setup complete"
