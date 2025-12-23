import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

DB_PATH = "C:/Users/FOM018/Desktop/ERP_Project/Data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

st.title("🚚 Delivery Performance & Insights")
st.set_page_config(
    layout="wide"
)

# --- KPI ---
kpi_query = """
SELECT
  COUNT(DISTINCT CASE WHEN delivery_date <= delivery_date THEN sales_order_id END) AS on_time,
  COUNT(DISTINCT CASE WHEN delivery_date > delivery_date THEN sales_order_id END) AS late,
  AVG(DATEDIFF('day', delivery_date, delivery_date)) AS avg_delay
FROM main_prod.fact_final_joined_files
"""
kpi_df = con.execute(kpi_query).df()
st.metric("On-Time Orders", kpi_df['on_time'][0])
st.metric("Late Orders", kpi_df['late'][0])
st.metric("Average Delay (days)", round(kpi_df['avg_delay'][0], 1))

# --- Chart: On-Time vs Late Orders ---
chart_df = pd.DataFrame({
    'Status': ['On-Time', 'Late'],
    'Orders': [kpi_df['on_time'][0], kpi_df['late'][0]]
})
fig = px.pie(chart_df, names='Status', values='Orders', title="Delivery Performance")
st.plotly_chart(fig, use_container_width=True)

# --- Top Customers by Order Value ---
customer_query = """
SELECT c.customer_name, SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name= c.customer_name
GROUP BY c.customer_name
ORDER BY total_value DESC
LIMIT 10
"""
customer_df = con.execute(customer_query).df()
fig2 = px.bar(customer_df, x='customer_name', y='total_value', title="Top Customers")
st.plotly_chart(fig2, use_container_width=True)

# --- Top Items by Demand ---
item_query = """
SELECT i.item_name, SUM(f.qty) AS total_qty
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
GROUP BY i.item_name
ORDER BY total_qty DESC
LIMIT 10
"""
item_df = con.execute(item_query).df()
fig3 = px.bar(item_df, x='item_name', y='total_qty', title="Top Items by Quantity Ordered")
st.plotly_chart(fig3, use_container_width=True)
