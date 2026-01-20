import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime

from utils.auth import require_auth   # ✅ unified auth guard
from utils.sidebar import render_sidebar

# =================== PAGE CONFIG ===================
st.set_page_config(layout="wide")

# =================== AUTH GUARD ===================
require_auth()   # 🔐 restores session from cookies on refresh

# =================== SIDEBAR FILTERS ===================
filters = render_sidebar()
if filters is None:
    st.stop()

selected_customers = filters["customers"]
selected_items = filters["items"]
delivery_status = filters["delivery_status"]
start_date = filters["start_date"]
end_date = filters["end_date"]
min_value = filters["min_value"]
so_search = filters["so_search"]

# =================== DB CONNECTION ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

# =================== TITLE ===================
st.title("📈 Sales Order – Current Trends")

# =================== WHERE CLAUSE BUILDER ===================
def build_where_clause(date_filter=True):
    clauses = ["f.open_qty > 0"]

    if selected_customers:
        customers_str = "', '".join(selected_customers)
        clauses.append(f"c.customer_name IN ('{customers_str}')")

    if selected_items:
        items_str = "', '".join(selected_items)
        clauses.append(f"i.item_name IN ('{items_str}')")

    if delivery_status != "All":
        if delivery_status == "On Time":
            clauses.append("f.delivery_date >= f.promised_date")
        elif delivery_status == "Overdue":
            clauses.append("f.delivery_date < f.promised_date")
        elif delivery_status == "Upcoming":
            clauses.append("f.promised_date > CURRENT_DATE")
        elif delivery_status == "Urgent (< 3 days)":
            clauses.append("f.promised_date <= CURRENT_DATE + INTERVAL 3 DAY")

    if min_value > 0:
        clauses.append(f"f.amount >= {min_value}")

    if so_search:
        clauses.append(f"f.sales_order_id LIKE '%{so_search}%'")

    if date_filter:
        clauses.append(f"f.order_date BETWEEN '{start_date}' AND '{end_date}'")

    return "WHERE " + " AND ".join(clauses)

# =================== KPI SECTION ===================
st.subheader("📊 Month-to-Date Performance")
k1, k2, k3 = st.columns(3)

kpi_query = f"""
SELECT
    SUM(f.amount) AS total_order_amount,
    COUNT(DISTINCT f.sales_order_id) AS total_orders,
    AVG(f.amount) AS avg_order_amount
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{build_where_clause(date_filter=False)}
AND f.order_date >= date_trunc('month', CURRENT_DATE)
"""
kpi_df = con.execute(kpi_query).df()

with k1:
    st.metric("Total Order Value (MTD)", kpi_df.iloc[0]["total_order_amount"])
with k2:
    st.metric("Orders This Month", kpi_df.iloc[0]["total_orders"])
with k3:
    st.metric("Average Order Value", round(kpi_df.iloc[0]["avg_order_amount"], 2))

# =================== ORDERS OVER TIME ===================
st.subheader("📈 Orders Over Time")

granularity = st.radio("View By", ["Daily", "Monthly", "Yearly"], horizontal=True)

date_col = (
    "f.order_date" if granularity == "Daily"
    else "date_trunc('month', f.order_date)" if granularity == "Monthly"
    else "date_trunc('year', f.order_date)"
)

time_query = f"""
SELECT
    {date_col} AS order_period,
    COUNT(DISTINCT f.sales_order_id) AS total_orders
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{build_where_clause()}
GROUP BY order_period
ORDER BY order_period
"""
time_df = con.execute(time_query).df()

fig_time = px.line(
    time_df,
    x="order_period",
    y="total_orders",
    markers=True,
    title="Orders Trend Over Time"
)
st.plotly_chart(fig_time, use_container_width=True)

# =================== TOP CUSTOMERS ===================
st.subheader("🏆 Top Customers")

customer_query = f"""
SELECT c.customer_name, SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{build_where_clause()}
GROUP BY c.customer_name
ORDER BY total_value DESC
LIMIT 10
"""
customer_df = con.execute(customer_query).df()

st.plotly_chart(
    px.bar(customer_df, x="customer_name", y="total_value",
           title="Top Customers by Sales Value"),
    use_container_width=True
)

# =================== TOP ITEMS ===================
st.subheader("📦 Top Items")

item_query = f"""
SELECT i.item_name, SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
{build_where_clause()}
GROUP BY i.item_name
ORDER BY total_value DESC
LIMIT 10
"""
item_df = con.execute(item_query).df()

c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(
        px.bar(item_df, x="item_name", y="total_value", title="Top Items by Sales"),
        use_container_width=True
    )
with c2:
    st.plotly_chart(
        px.pie(item_df, names="item_name", values="total_value",
               title="Item Contribution", hole=0.4),
        use_container_width=True
    )

# =================== RECENT ORDERS ===================
st.subheader("📋 Recent Orders")

orders_query = f"""
SELECT
    f.sales_order_id,
    c.customer_name,
    i.item_name,
    f.order_date,
    f.amount,
    f.order_status
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{build_where_clause()}
ORDER BY f.order_date DESC
"""
orders_df = con.execute(orders_query).df()

st.dataframe(orders_df, use_container_width=True)

con.close()
