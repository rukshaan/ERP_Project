import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime
from streamlit_plotly_events import plotly_events

from utils.auth import require_auth
from utils.sidebar import render_sidebar

# =================== PAGE CONFIG ===================
st.set_page_config(layout="wide")
require_auth()

# =================== DB CONNECTION ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

# =================== SIDEBAR FILTERS ===================
filters = render_sidebar()
if filters is None:
    st.stop()

# Shared filters
start_date = filters["start_date"]
end_date = filters["end_date"]
selected_customers = filters["customers"]
selected_items = filters["items"]
delivery_status = filters["delivery_status"]
min_value = filters["min_value"]
so_search = filters["so_search"]

# =================== TABS ===================
tab1, tab2 = st.tabs(["📈 Sales Overview", "📋 Backlog / Open Orders"])

# =================== TAB 1: SALES OVERVIEW ===================
with tab1:
    st.title("📈 Sales Order – Current Trends")

    # --- WHERE CLAUSE FUNCTION ---
    def build_where_clause(date_filter=True):
        clauses = ["f.open_qty > 0"]
        if selected_customers:
            clauses.append(f"c.customer_name IN ('{'',''.join(selected_customers)}')")
        if selected_items:
            clauses.append(f"i.item_name IN ('{'',''.join(selected_items)}')")
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

    # --- KPI SECTION ---
    st.subheader("📊 Month-to-Date Performance")
    k1, k2, k3 = st.columns(3)
    kpi_query = f"""
        SELECT SUM(f.amount) AS total_order_amount,
               COUNT(DISTINCT f.sales_order_id) AS total_orders,
               AVG(f.amount) AS avg_order_amount
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        {build_where_clause(date_filter=False)}
        AND f.order_date >= date_trunc('month', CURRENT_DATE)
    """
    kpi_df = con.execute(kpi_query).df()
    k1.metric("Total Order Value (MTD)", kpi_df.iloc[0]["total_order_amount"])
    k2.metric("Orders This Month", kpi_df.iloc[0]["total_orders"])
    k3.metric("Average Order Value", round(kpi_df.iloc[0]["avg_order_amount"], 2))

    # --- ORDERS OVER TIME ---
    st.subheader("📈 Orders Over Time")
    granularity = st.radio("View By", ["Daily", "Monthly", "Yearly"], horizontal=True)
    date_col = (
        "f.order_date" if granularity == "Daily"
        else "date_trunc('month', f.order_date)" if granularity == "Monthly"
        else "date_trunc('year', f.order_date)"
    )
    time_query = f"""
        SELECT {date_col} AS order_period,
               COUNT(DISTINCT f.sales_order_id) AS total_orders
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        {build_where_clause()}
        GROUP BY order_period
        ORDER BY order_period
    """
    time_df = con.execute(time_query).df()
    fig_time = px.line(time_df, x="order_period", y="total_orders",
                       markers=True, title="Orders Trend Over Time")
    st.plotly_chart(fig_time, use_container_width=True)

    # --- TOP CUSTOMERS ---
    st.subheader("🏆 Top Customers")
    customer_query = f"""
        SELECT c.customer_name, SUM(f.amount) AS total_value
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        {build_where_clause()}
        GROUP BY c.customer_name
        ORDER BY total_value DESC
    """
    customer_df = con.execute(customer_query).df()
    st.dataframe(customer_df, use_container_width=True, height=400)

    # --- TOP ITEMS ---
    st.subheader("📦 Top Items")
    item_query = f"""
        SELECT i.item_name, SUM(f.amount) AS total_value
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        {build_where_clause()}
        GROUP BY i.item_name
        ORDER BY total_value DESC
    """
    item_df = con.execute(item_query).df()
    st.dataframe(item_df, use_container_width=True, height=400)

    # --- RECENT ORDERS ---
    st.subheader("📋 Recent Orders")
    orders_query = f"""
        SELECT f.sales_order_id, c.customer_name, i.item_name,
               f.order_date, f.amount, f.order_status
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        {build_where_clause()}
        ORDER BY f.order_date DESC
    """
    orders_df = con.execute(orders_query).df()
    st.dataframe(orders_df, use_container_width=True, height=400)

# =================== TAB 2: BACKLOG / OPEN ORDERS ===================
with tab2:
    st.title("📋 Backlog / Open Orders Dashboard")

    # --- WHERE CLAUSE FOR BACKLOG ---
    filter_conditions = ["f.open_qty > 0"]
    filter_conditions.append(f"f.order_date BETWEEN '{start_date}' AND '{end_date}'")
    if selected_customers:
        filter_conditions.append(f"c.customer_name IN ('{'',''.join(selected_customers)}')")
    if selected_items:
        filter_conditions.append(f"i.item_name IN ('{'',''.join(selected_items)}')")

    current_date = datetime.now().strftime('%Y-%m-%d')
    if delivery_status == "Overdue":
        filter_conditions.append(f"f.delivery_date < '{current_date}'")
    elif delivery_status == "Urgent (< 3 days)":
        filter_conditions.append(f"f.delivery_date BETWEEN '{current_date}' AND DATE '{current_date}' + INTERVAL 3 DAYS")
    elif delivery_status == "Upcoming":
        filter_conditions.append(f"f.delivery_date > DATE '{current_date}' + INTERVAL 3 DAYS")
    if min_value > 0:
        filter_conditions.append(f"(f.open_qty * f.rate) >= {min_value}")
    if so_search:
        filter_conditions.append(f"f.sales_order_id LIKE '%{so_search}%'")
    where_clause = " AND ".join(filter_conditions)

    # --- KPI SECTION ---
    st.header("📊 Key Performance Indicators")
    kpi_query = f"""
        SELECT COALESCE(SUM(f.open_qty * f.rate),0) AS total_open_value,
               COALESCE(COUNT(DISTINCT f.sales_order_id),0) AS open_orders,
               COALESCE(AVG(DATEDIFF('day', f.order_date, f.delivery_date)),0) AS avg_open_days
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        WHERE {where_clause}
    """
    kpi_df = con.execute(kpi_query).df()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Open Value", f"₹{kpi_df['total_open_value'][0]:,.0f}")
    col2.metric("Open Orders", int(kpi_df['open_orders'][0]))
    col3.metric("Avg Open Days", f"{kpi_df['avg_open_days'][0]:.1f}")

    # --- FULL TABLE OF OPEN ORDERS ---
    table_query = f"""
        SELECT f.sales_order_id, c.customer_name, i.item_name,
               f.qty AS ordered_qty, f.open_qty AS pending_qty,
               f.rate, (f.open_qty * f.rate) AS pending_amount,
               f.order_date, f.delivery_date,
               CASE 
                   WHEN f.delivery_date < CURRENT_DATE THEN 'Overdue'
                   WHEN f.delivery_date <= DATE_ADD(CURRENT_DATE, 3) THEN 'Urgent'
                   ELSE 'On Track'
               END AS delivery_status
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        WHERE {where_clause}
        ORDER BY f.delivery_date ASC
    """
    open_orders_df = con.execute(table_query).df()
    st.subheader("📋 Detailed Open Orders")
    st.dataframe(open_orders_df, use_container_width=True, height=600)

# =================== CLOSE CONNECTION ===================
con.close()
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Filters applied to all sections above")