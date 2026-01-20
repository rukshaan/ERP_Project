# app.py
import streamlit as st
import duckdb
from utils.auth import require_auth   # ✅ ADD THIS
from utils.sidebar import render_sidebar

DB_PATH = "./data/Silver/dev.duckdb"

st.set_page_config(page_title="ERP Dashboard", layout="wide")

# 🔐 THIS IS THE KEY LINE
require_auth()          # ← restores session from cookies on refresh

# Sidebar AFTER auth
render_sidebar()

con = duckdb.connect(DB_PATH, read_only=False)

st.title("ERP Dashboard")

df = con.execute("""
    SELECT *
    FROM main_prod.fact_final_joined_files
    ORDER BY order_date DESC
""").df()

st.dataframe(df)
