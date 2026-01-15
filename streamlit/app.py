import streamlit as st
import duckdb
from utils.sidebar import render_sidebar
import plotly.express as px

from login import render_login


st.set_page_config(page_title="ERP Dashboard", layout="wide")

# Init auth state
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# 🔒 NOT LOGGED IN → SHOW LOGIN ONLY
if not st.session_state.authenticated:
    render_login()
    st.stop()
    st.write("SESSION:", st.session_state)
DB_PATH = "./data/Silver/dev.duckdb"
# DB_PATH = r"C:\Users\FOM018\Desktop\ERP_Project\Data\Silver\dev.duckdb"



# ✅ LOGGED IN → DASHBOARD
filters = render_sidebar()

con = duckdb.connect(DB_PATH, read_only=False)

# # First, show all schemas
# st.subheader("Available Schemas")
# schemas = con.execute("SHOW TABLES").df()
# st.dataframe(schemas)

# Show all tables in all schemas
# st.subheader("All Tables in Database")
# all_tables = con.execute("""
#     SELECT 
#         table_schema as schema_name,
#         table_name
#     FROM information_schema.tables
#     ORDER BY table_schema, table_name
# """).df()
# st.dataframe(all_tables)

# Query from the specific schema
df = con.execute("""
    SELECT *
    FROM main_prod.fact_final_joined_files
    ORDER BY order_date DESC
    
""").df()

st.title("ERP Dashboard")
# st.write(f"Retrieved {len(df)} rows from main_prod.fact_final_joined_files")
st.dataframe(df)

