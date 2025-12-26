import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# ---------------- DB CONNECTION ----------------
DB_PATH = "./data/Silver/dev.duckdb"
# DB_PATH = "C:/Users/FOM018/Desktop/ERP_Project/Data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=False)

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
    st.metric("Orders in Each Month", kpi_df['total_orders'][0])

with kpi_col3:
    st.metric("Average Order Value", round(kpi_df['avg_order_amount'][0], 2))

# ==================================================
# 🔹 ORDERS OVER TIME
# ==================================================
st.subheader("📈 Orders Over Time")
f1, f2, f3, f4 = st.columns(4)

with f1:
    time_granularity = st.radio(
        "View By",
        ["Daily", "Monthly", "Yearly"],
        horizontal=True
    )

with f2:
    time_customer = st.multiselect("Customer", customers, key="time_customer")

with f3:
    time_item = st.multiselect("Item", items, key="time_item")

with f4:
    time_status = st.multiselect("Order Status", statuses, key="time_status")

# Build dynamic filters
time_filters = []
if time_customer:
    time_filters.append(
        f"c.customer_name IN ('{' ,'.join(time_customer)}')".replace(" ,", "', '")
    )
if time_item:
    time_filters.append(
        f"i.item_name IN ('{' ,'.join(time_item)}')".replace(" ,", "', '")
    )
if time_status:
    time_filters.append(
        f"f.order_status IN ('{' ,'.join(time_status)}')".replace(" ,", "', '")
    )

time_where = "WHERE " + " AND ".join(time_filters) if time_filters else ""

# Date granularity
if time_granularity == "Daily":
    date_column = "f.order_date"
elif time_granularity == "Monthly":
    date_column = "date_trunc('month', f.order_date)"
else:  # Yearly
    date_column = "date_trunc('year', f.order_date)"

# Orders over time query
time_query = f"""
SELECT
    {date_column} AS order_period,
    COUNT(DISTINCT f.sales_order_id) AS total_orders
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{time_where}
GROUP BY order_period
ORDER BY order_period
"""
time_df = con.execute(time_query).df()

# Plot chart
fig_time = px.line(
    time_df,
    x="order_period",
    y="total_orders",
    markers=True,
    title="Orders Trend Over Time",
    labels={"order_period": "Date", "total_orders": "Number of Orders"}
)
fig_time.update_layout(hovermode="x unified", xaxis_title="Date", yaxis_title="Orders")
st.plotly_chart(fig_time, use_container_width=True)



# ==================================================
# 🔹 TOP CUSTOMERS
# ==================================================
st.subheader("🏆 Top Customers")
c1, c2 = st.columns(2)

with c1:
    customer_filter = st.multiselect("Customer", customers, key="customer_chart_filter")

with c2:
    status_filter = st.multiselect("Order Status", statuses, key="customer_chart_status")

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
fig = px.bar(chart_df, x="customer_name", y="total_value", title="Top Customers by Total Sales")
st.plotly_chart(fig, use_container_width=True)


# ==================================================
# 🔹 TOP ITEMS
# ==================================================
st.subheader("📦 Top Items")
i1, i2 = st.columns(2)

with i1:
    item_filter = st.multiselect("Item", items, key="item_chart_filter")
with i2:
    item_status_filter = st.multiselect("Order Status", statuses, key="item_chart_status")

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

b1, b2 = st.columns(2)
with b1:
    fig2 = px.bar(item_df, x="item_name", y="total_value", title="Top Items by Total Sales")
    st.plotly_chart(fig2, use_container_width=True)

with b2:
    pie_fig = px.pie(
        item_df,
        names="item_name",
        values="total_value",
        title="Item Contribution to Total Sales",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    pie_fig.update_traces(textposition="inside", textinfo="percent+label")
    pie_fig.update_layout(legend_title_text="Items", height=500)
    st.plotly_chart(pie_fig, use_container_width=True)


# ==================================================
# 🔹 ORDERS TABLE
# ==================================================
st.subheader("📋 Recent Orders – Live View")
t1, t2, t3 = st.columns(3)

with t1:
    table_customer = st.multiselect("Customer", customers, key="table_customer")
with t2:
    table_item = st.multiselect("Item", items, key="table_item")
with t3:
    table_status = st.multiselect("Order Status", statuses, key="table_status")

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
