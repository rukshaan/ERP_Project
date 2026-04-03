import duckdb
import psycopg2
import os
import streamlit as st

DB_PATH = "./data/Silver/dev.duckdb"

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

def get_pg_connection():
    """Returns a connection to the PostgreSQL database."""
    # Priority: Env Var > 'postgres' (if in docker) > 'localhost'
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    dbname = os.getenv("POSTGRES_DB", "airflow")
    port = os.getenv("POSTGRES_PORT", "5432")

    # If host not explicitly set, try 'postgres' first then 'localhost'
    hosts_to_try = [host] if host else ["postgres", "localhost"]
    
    last_exception = None
    for h in hosts_to_try:
        try:
            conn = psycopg2.connect(
                host=h,
                database=dbname,
                user=user,
                password=password,
                port=port,
                connect_timeout=5
            )
            return conn
        except Exception as e:
            last_exception = e
            continue
            
    # If all failed, raise a detailed error
    error_msg = f"Failed to connect to PostgreSQL (tried {', '.join(hosts_to_try)}). Original error: {str(last_exception)}"
    print(f"❌ Database Connection Error: {error_msg}")
    raise Exception(error_msg)
