import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

DB_PATH = "C:/Users/FOM018/Desktop/ERP_Project/Data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

st.title("📋 Backlog / Open Orders")

# --- KPI ---
kpi_query = """
SELECT SUM(open_qty * rate) AS total_open_value,
       COUNT(DISTINCT sales_order_id) AS open_orders,
       AVG(DATEDIFF('day', order_date, delivery_date)) AS avg_open_days
FROM main_prod.fact_final_joined_files
WHERE open_qty > 0
"""
kpi_df = con.execute(kpi_query).df()
st.metric("Total Open Order Value", kpi_df['total_open_value'][0])
st.metric("Number of Open Orders", kpi_df['open_orders'][0])
st.metric("Average Open Days", round(kpi_df['avg_open_days'][0], 1))

# --- Charts ---
# Open order value by customer
chart_query = """
SELECT c.customer_name, SUM(f.open_qty * f.rate) AS open_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
WHERE f.open_qty > 0
GROUP BY c.customer_name
ORDER BY open_value DESC
LIMIT 10
"""
chart_df = con.execute(chart_query).df()
fig = px.bar(chart_df, x='customer_name', y='open_value', title="Open Order Value by Customer")
st.plotly_chart(fig, use_container_width=True)

# Open order value by item
item_query = """
SELECT i.item_name, SUM(f.open_qty * f.rate) AS open_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE f.open_qty > 0
GROUP BY i.item_name
ORDER BY open_value DESC
LIMIT 10
"""
item_df = con.execute(item_query).df()
fig2 = px.bar(item_df, x='item_name', y='open_value', title="Open Order Value by Item")
st.plotly_chart(fig2, use_container_width=True)

# --- Table: Open Orders ---
table_query = """
SELECT f.sales_order_id, c.customer_name, i.item_name, f.qty, f.delivery_qty,
       (f.qty - f.delivery_qty) AS open_qty,
       (f.qty - f.delivery_qty) * f.rate AS open_amount,
       f.delivery_date
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE f.qty - f.delivery_qty > 0
ORDER BY f.delivery_date ASC
LIMIT 100
"""
table_df = con.execute(table_query).df()
st.dataframe(table_df)
