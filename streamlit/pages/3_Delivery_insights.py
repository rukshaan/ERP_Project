import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime
from utils.sidebar import render_sidebar
from login import render_login
# =================== PAGE CONFIG ===================


# =================== PAGE CONFIG ===================
st.set_page_config(layout="wide")

# 🔐 AUTH GUARD
if not st.session_state.get("authenticated", False):
    render_login()
    st.stop()

# =================== DB CONNECTION ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=False)

# =================== SIDEBAR FILTERS ===================
filters = render_sidebar()

st.title("🚚 Delivery Performance & Insights")

# =================== WHERE CLAUSE BUILDER ===================
def build_filter_clause(filters: dict):
    clauses = ["f.open_qty > 0"]

    # Date filter
    clauses.append(
        f"f.order_date BETWEEN '{filters['start_date']}' AND '{filters['end_date']}'"
    )

    # Customer filter
    if filters['customers']:
        customers_str = "', '".join(filters['customers'])
        clauses.append(f"f.customer_name IN ('{customers_str}')")

    # Item filter
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

    # Delivery status
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


where_clause = build_filter_clause(filters)

# =================== KPI SECTION ===================
kpi_query = f"""
SELECT
    COUNT(DISTINCT CASE WHEN f.delivery_date <= f.order_date THEN f.sales_order_id END) AS on_time,
    COUNT(DISTINCT CASE WHEN f.delivery_date > f.order_date THEN f.sales_order_id END) AS late,
    AVG(DATEDIFF('day', f.order_date, f.delivery_date)) AS avg_delay
FROM main_prod.fact_final_joined_files f
WHERE {where_clause}
"""
kpi_df = con.execute(kpi_query).df()

col1, col2, col3 = st.columns(3)
col1.metric("On-Time Orders", kpi_df['on_time'][0] if not kpi_df.empty else 0)
col2.metric("Late Orders", kpi_df['late'][0] if not kpi_df.empty else 0)
col3.metric("Average Delay (days)", round(kpi_df['avg_delay'][0], 1) if not kpi_df.empty else 0)

# =================== PIE CHART ===================
chart_df = pd.DataFrame({
    'Status': ['On-Time', 'Late'],
    'Orders': [
        kpi_df['on_time'][0] if not kpi_df.empty else 0,
        kpi_df['late'][0] if not kpi_df.empty else 0
    ]
})

fig_pie = px.pie(
    chart_df,
    names='Status',
    values='Orders',
    title="Delivery Performance"
)
st.plotly_chart(fig_pie, use_container_width=True)

# =================== TOP CUSTOMERS ===================
st.subheader("🏆 Top Customers")

customer_query = f"""
SELECT
    f.customer_name,
    SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
WHERE {where_clause}
GROUP BY f.customer_name
ORDER BY total_value DESC
LIMIT 10
"""
customer_df = con.execute(customer_query).df()

if customer_df.empty:
    st.info("No customer data available for selected filters.")
    st.stop()

fig_customers = px.bar(
    customer_df,
    x='customer_name',
    y='total_value',
    title="Top Customers by Order Value"
)
st.plotly_chart(fig_customers, use_container_width=True)

# =================== CUSTOMER DRILL-DOWN ===================
st.subheader("📦 Customer Item Breakdown")

selected_customer = st.selectbox(
    "Select a customer",
    customer_df['customer_name'].unique()
)

customer_item_query = f"""
SELECT
    i.item_name,
    SUM(f.qty) AS total_qty,
    SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i
    ON f.item_code = i.item_code
WHERE {where_clause}
  AND f.customer_name = '{selected_customer}'
GROUP BY i.item_name
ORDER BY total_qty DESC
"""
customer_item_df = con.execute(customer_item_query).df()

if not customer_item_df.empty:
    fig_items = px.pie(
        customer_item_df,
        names='item_name',
        values='total_qty',
        title=f"Item Quantity Share for {selected_customer}",
        hole=0.3  # remove this line if you want a full pie instead of donut
    )
    
    fig_items.update_traces(textinfo='percent+label')
    
    st.plotly_chart(fig_items, use_container_width=True)

else:
    st.info("No item data available for selected customer.")


# =================== TOP ITEMS OVERALL ===================
st.subheader("🔥 Top Items by Quantity")

item_query = f"""
SELECT
    i.item_name,
    SUM(f.qty) AS total_qty
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE {where_clause}
GROUP BY i.item_name
ORDER BY total_qty DESC
LIMIT 10
"""
item_df = con.execute(item_query).df()

if not item_df.empty:
    fig_items_all = px.bar(
        item_df,
        x='item_name',
        y='total_qty',
        title="Top Items by Quantity Ordered"
    )
    st.plotly_chart(fig_items_all, use_container_width=True)
