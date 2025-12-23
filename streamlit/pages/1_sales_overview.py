import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# ---------------- DB CONNECTION ----------------
DB_PATH = "C:/Users/FOM018/Desktop/ERP_Project/Data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

st.set_page_config(page_title="Sales Current Trends", layout="wide")
st.title("📈 Sales Order – Current Trends")

# ---------------- LOAD FILTER VALUES ----------------
customers = con.execute("""
    SELECT DISTINCT customer_name
    FROM main_prod.dim_customer
    ORDER BY customer_name
""").df()["customer_name"].tolist()

items = con.execute("""
    SELECT DISTINCT item_name
    FROM main_prod.dim_item
    ORDER BY item_name
""").df()["item_name"].tolist()

statuses = con.execute("""
    SELECT DISTINCT order_status
    FROM main_prod.fact_final_joined_files
    ORDER BY order_status
""").df()["order_status"].tolist()

# ==================================================
# 🔹 KPI SECTION (CURRENT MONTH TRENDS)
# ==================================================
st.subheader("📊 Month-to-Date Performance")

kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

kpi_query = """
SELECT
    SUM(amount) AS total_order_amount,
    COUNT(DISTINCT sales_order_id) AS total_orders,
    AVG(amount) AS avg_order_amount
FROM main_prod.fact_final_joined_files
WHERE order_date >= date_trunc('month', CURRENT_DATE)
"""

kpi_df = con.execute(kpi_query).df()

with kpi_col1:
    st.metric("Total Order Value (MTD)", kpi_df['total_order_amount'][0])

with kpi_col2:
    st.metric("Orders in each months", kpi_df['total_orders'][0])

with kpi_col3:
    st.metric("Average Order Value", round(kpi_df['avg_order_amount'][0], 2))


# ==================================================
# 🔹 TOP CUSTOMERS (WITH OWN FILTERS)
# ==================================================
st.subheader("🏆 Top Customers")

c1, c2 = st.columns(2)

with c1:
    customer_filter = st.multiselect(
        "Customer",
        customers,
        key="customer_chart_filter"
    )

with c2:
    status_filter = st.multiselect(
        "Order Status",
        statuses,
        key="customer_chart_status"
    )

filters = []

if customer_filter:
    filters.append(f"c.customer_name IN ('{' ,'.join(customer_filter)}')".replace(" ,", "', '"))

if status_filter:
    filters.append(f"f.order_status IN ('{' ,'.join(status_filter)}')".replace(" ,", "', '"))

where_clause = "WHERE " + " AND ".join(filters) if filters else ""

chart_query = f"""
SELECT c.customer_name, SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
{where_clause}
GROUP BY c.customer_name
ORDER BY total_value DESC
LIMIT 10
"""

chart_df = con.execute(chart_query).df()
fig = px.bar(chart_df, x="customer_name", y="total_value")
st.plotly_chart(fig, use_container_width=True)


# ==================================================
# 🔹 TOP ITEMS (WITH OWN FILTERS)
# ==================================================
st.subheader("📦 Top Items")

i1, i2 = st.columns(2)

with i1:
    item_filter = st.multiselect(
        "Item",
        items,
        key="item_chart_filter"
    )

with i2:
    item_status_filter = st.multiselect(
        "Order Status",
        statuses,
        key="item_chart_status"
    )

item_filters = []

if item_filter:
    item_filters.append(f"i.item_name IN ('{' ,'.join(item_filter)}')".replace(" ,", "', '"))

if item_status_filter:
    item_filters.append(f"f.order_status IN ('{' ,'.join(item_status_filter)}')".replace(" ,", "', '"))

item_where = "WHERE " + " AND ".join(item_filters) if item_filters else ""

item_query = f"""
SELECT i.item_name, SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{item_where}
GROUP BY i.item_name
ORDER BY total_value DESC
LIMIT 10
"""

item_df = con.execute(item_query).df()
fig2 = px.bar(item_df, x="item_name", y="total_value")
st.plotly_chart(fig2, use_container_width=True)


# ==================================================
# 🔹 ORDERS TABLE (WITH OWN FILTERS)
# ==================================================
st.subheader("📋 Recent Orders – Live View")

t1, t2, t3 = st.columns(3)

with t1:
    table_customer = st.multiselect(
        "Customer",
        customers,
        key="table_customer"
    )

with t2:
    table_item = st.multiselect(
        "Item",
        items,
        key="table_item"
    )

with t3:
    table_status = st.multiselect(
        "Order Status",
        statuses,
        key="table_status"
    )

table_filters = []

if table_customer:
    table_filters.append(f"c.customer_name IN ('{' ,'.join(table_customer)}')".replace(" ,", "', '"))

if table_item:
    table_filters.append(f"i.item_name IN ('{' ,'.join(table_item)}')".replace(" ,", "', '"))

if table_status:
    table_filters.append(f"f.order_status IN ('{' ,'.join(table_status)}')".replace(" ,", "', '"))

table_where = "WHERE " + " AND ".join(table_filters) if table_filters else ""

table_query = f"""
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
{table_where}
ORDER BY f.order_date DESC

"""

table_df = con.execute(table_query).df()
st.dataframe(table_df, use_container_width=True)
