import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

DB_PATH = "C:/Users/FOM018/Desktop/ERP_Project/Data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

st.title("📈 Sales Order Overview")

# --- KPI: Total Order Value (MTD) ---
kpi_query = """
SELECT SUM(order_value) AS total_order_value,
       COUNT(DISTINCT order_id) AS total_orders,
       AVG(order_value) AS avg_order_value
FROM main_prod.fact_final_joined_files
WHERE order_date >= date_trunc('month', CURRENT_DATE)
"""
kpi_df = con.execute(kpi_query).df()
st.metric("Total Order Value (MTD)", kpi_df['total_order_value'][0])
st.metric("Number of Orders", kpi_df['total_orders'][0])
st.metric("Average Order Value", round(kpi_df['avg_order_value'][0], 2))

# --- Chart: Orders by Customer ---
chart_query = """
SELECT c.customer_name, SUM(f.order_value) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_value DESC
LIMIT 10
"""
chart_df = con.execute(chart_query).df()
fig = px.bar(chart_df, x='customer_name', y='total_value', title="Top Customers")
st.plotly_chart(fig, use_container_width=True)

# --- Chart: Orders by Item ---
item_query = """
SELECT i.item_name, SUM(f.order_value) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_id = i.item_id
GROUP BY i.item_name
ORDER BY total_value DESC
LIMIT 10
"""
item_df = con.execute(item_query).df()
fig2 = px.bar(item_df, x='item_name', y='total_value', title="Top Items")
st.plotly_chart(fig2, use_container_width=True)

# --- Table: All Orders ---
table_query = """
SELECT f.order_id, c.customer_name, f.order_date, f.order_value, f.status
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_id = c.customer_id
ORDER BY f.order_date DESC
LIMIT 100
"""
table_df = con.execute(table_query).df()
st.dataframe(table_df)
