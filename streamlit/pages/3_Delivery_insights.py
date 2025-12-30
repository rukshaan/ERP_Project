import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime
from utils.sidebar import render_sidebar

st.set_page_config(layout="wide")

# =================== DB CONNECTION ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=False)

# --- Render sidebar and get filters ---
filters = render_sidebar()

st.title("🚚 Delivery Performance & Insights")

# --- Helper function to build WHERE clauses with fully qualified columns ---
def build_filter_clause(filters: dict):
    clauses = ["f.open_qty > 0"]

    # Date filter (always applied)
    clauses.append(
        f"f.order_date BETWEEN '{filters['start_date']}' AND '{filters['end_date']}'"
    )

    # Customer filter (ONLY if selected)
    if filters['customers']:
        customers_str = "', '".join(filters['customers'])
        clauses.append(f"f.customer_name IN ('{customers_str}')")

    # Item filter (ONLY if selected)
    if filters['items']:
        items_str = "', '".join(filters['items'])
        clauses.append(
            f"""
            f.item_code IN (
                SELECT i.item_code
                FROM main_prod.dim_item i
                WHERE i.item_name IN ('{items_str}')
            )
            """
        )

    # Delivery status filter
    today = datetime.now().date()
    if filters['delivery_status'] == "On Time":
        clauses.append("f.delivery_date <= f.order_date")
    elif filters['delivery_status'] == "Overdue":
        clauses.append("f.delivery_date > f.order_date")
    elif filters['delivery_status'] == "Upcoming":
        clauses.append(f"f.delivery_date > '{today}'")
    elif filters['delivery_status'] == "Urgent (< 3 days)":
        clauses.append(f"DATEDIFF('day', '{today}', f.delivery_date) <= 3")

    # Minimum order value
    if filters['min_value'] > 0:
        clauses.append(f"f.amount >= {filters['min_value']}")

    # Sales order search
    if filters['so_search']:
        clauses.append(f"f.sales_order_id LIKE '%{filters['so_search']}%'")

    return " AND ".join(clauses)

# --- KPI ---  
where_clause = build_filter_clause(filters)
kpi_query = f"""
SELECT
    COUNT(DISTINCT CASE WHEN f.delivery_date <= f.order_date THEN f.sales_order_id END) AS on_time,
    COUNT(DISTINCT CASE WHEN f.delivery_date > f.order_date THEN f.sales_order_id END) AS late,
    AVG(DATEDIFF('day', f.order_date, f.delivery_date)) AS avg_delay
FROM main_prod.fact_final_joined_files f
WHERE {where_clause}
"""
kpi_df = con.execute(kpi_query).df()

st.metric("On-Time Orders", kpi_df['on_time'][0] if not kpi_df.empty else 0)
st.metric("Late Orders", kpi_df['late'][0] if not kpi_df.empty else 0)
st.metric("Average Delay (days)", round(kpi_df['avg_delay'][0], 1) if not kpi_df.empty else 0)

# --- Chart: On-Time vs Late Orders ---
chart_df = pd.DataFrame({
    'Status': ['On-Time', 'Late'],
    'Orders': [kpi_df['on_time'][0] if not kpi_df.empty else 0,
               kpi_df['late'][0] if not kpi_df.empty else 0]
})
fig = px.pie(chart_df, names='Status', values='Orders', title="Delivery Performance")
st.plotly_chart(fig, use_container_width=True)

# --- Top Customers by Order Value ---
customer_query = f"""
SELECT c.customer_name, SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
WHERE {where_clause}
GROUP BY c.customer_name
ORDER BY total_value DESC
LIMIT 10
"""
customer_df = con.execute(customer_query).df()
if not customer_df.empty:
    fig2 = px.bar(customer_df, x='customer_name', y='total_value', title="Top Customers")
    st.plotly_chart(fig2, use_container_width=True)

# --- Top Items by Demand ---
item_query = f"""
SELECT i.item_name, SUM(f.qty) AS total_qty
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE {where_clause}
GROUP BY i.item_name
ORDER BY total_qty DESC
LIMIT 10
"""
item_df = con.execute(item_query).df()
if not item_df.empty:
    fig3 = px.bar(item_df, x='item_name', y='total_qty', title="Top Items by Quantity Ordered")
    st.plotly_chart(fig3, use_container_width=True)
