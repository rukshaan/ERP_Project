import duckdb
import streamlit as st

DB_PATH = "./data/Silver/dev.duckdb"

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=False)
