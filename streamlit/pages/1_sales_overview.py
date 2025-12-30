import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.sidebar import render_sidebar
from datetime import datetime
st.set_page_config(layout="wide")
# ---------------- DB CONNECTION ----------------
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=False)

# ---------------- GLOBAL SIDEBAR FILTERS ----------------
filters = render_sidebar()

selected_customers = filters['customers']
selected_items = filters['items']
delivery_status = filters['delivery_status']
start_date = filters['start_date']
end_date = filters['end_date']
min_value = filters['min_value']
so_search = filters['so_search']

# ---------------- TITLE ----------------
st.title("📈 Sales Order – Current Trends")

# ---------------- DYNAMIC WHERE CLAUSE FUNCTION ----------------
def build_where_clause(date_filter=True):
    where_clauses = ["f.open_qty > 0"]

    if selected_customers:
        customers_str = "', '".join(selected_customers)
        where_clauses.append(f"c.customer_name IN ('{customers_str}')")

    if selected_items:
        items_str = "', '".join(selected_items)
        where_clauses.append(f"i.item_name IN ('{items_str}')")

    if delivery_status != "All":
        if delivery_status == "On Time":
            where_clauses.append("f.delivery_date >= f.promised_date")
        elif delivery_status == "Overdue":
            where_clauses.append("f.delivery_date < f.promised_date")
        elif delivery_status == "Upcoming":
            where_clauses.append("f.promised_date > CURRENT_DATE")
        elif delivery_status == "Urgent (< 3 days)":
            where_clauses.append("f.promised_date <= CURRENT_DATE + INTERVAL 3 DAY")

    if min_value > 0:
        where_clauses.append(f"f.amount >= {min_value}")

    if so_search:
        where_clauses.append(f"f.sales_order_id LIKE '%{so_search}%'")

    if date_filter:
        where_clauses.append(f"f.order_date BETWEEN '{start_date}' AND '{end_date}'")

    return "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

# ---------------- KPI SECTION ----------------
st.subheader("📊 Month-to-Date Performance")
kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

kpi_query = f"""
SELECT
    SUM(amount) AS total_order_amount,
    COUNT(DISTINCT sales_order_id) AS total_orders,
    AVG(amount) AS avg_order_amount
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{build_where_clause(date_filter=False)}
AND f.order_date >= date_trunc('month', CURRENT_DATE)
"""
kpi_df = con.execute(kpi_query).df()

with kpi_col1:
    st.metric("Total Order Value (MTD)", kpi_df['total_order_amount'][0])

with kpi_col2:
    st.metric("Orders in Each Month", kpi_df['total_orders'][0])

with kpi_col3:
    st.metric("Average Order Value", round(kpi_df['avg_order_amount'][0], 2))

# ---------------- ORDERS OVER TIME ----------------
st.subheader("📈 Orders Over Time")
f1, f2, f3 = st.columns(3)

with f1:
    time_granularity = st.radio(
        "View By",
        ["Daily", "Monthly", "Yearly"],
        horizontal=True
    )

# Date granularity
if time_granularity == "Daily":
    date_column = "f.order_date"
elif time_granularity == "Monthly":
    date_column = "date_trunc('month', f.order_date)"
else:  # Yearly
    date_column = "date_trunc('year', f.order_date)"

time_query = f"""
SELECT
    {date_column} AS order_period,
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
    title="Orders Trend Over Time",
    labels={"order_period": "Date", "total_orders": "Number of Orders"}
)
fig_time.update_layout(hovermode="x unified", xaxis_title="Date", yaxis_title="Orders")
st.plotly_chart(fig_time, use_container_width=True)

# ---------------- TOP CUSTOMERS ----------------
st.subheader("🏆 Top Customers")
chart_query = f"""
SELECT c.customer_name, SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{build_where_clause()}
GROUP BY c.customer_name
ORDER BY total_value DESC
LIMIT 10
"""
chart_df = con.execute(chart_query).df()
fig = px.bar(chart_df, x="customer_name", y="total_value", title="Top Customers by Total Sales")
st.plotly_chart(fig, use_container_width=True)

# ---------------- TOP ITEMS ----------------
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

# ---------------- ORDERS TABLE ----------------
st.subheader("📋 Recent Orders – Live View")
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
{build_where_clause()}
ORDER BY f.order_date DESC
"""
table_df = con.execute(table_query).df()
st.dataframe(table_df, use_container_width=True)

# ---------------- WORLD MAP ----------------
st.subheader("🌍 Global Sales Distribution")

map_query = f"""
SELECT
    c.customer_name,
    c.territory,
    SUM(f.amount) AS total_sales,
    COUNT(DISTINCT f.sales_order_id) AS total_orders,
    AVG(f.amount) AS avg_order_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
{build_where_clause()}
GROUP BY c.customer_name, c.territory
HAVING SUM(f.amount) > 0
"""

map_df = con.execute(map_query).df()

if map_df.empty:
    st.warning("No data available for world map.")
else:
    # Aggregate per territory
    territory_df = map_df.groupby('territory').agg({
        'total_sales': 'sum',
        'total_orders': 'sum',
        'customer_name': 'nunique'
    }).reset_index().rename(columns={'customer_name': 'customer_count'})

    # Scale marker size based on sales
    max_sales = territory_df['total_sales'].max()
    min_sales = territory_df['total_sales'].min()
    territory_df['marker_size'] = 15 + (territory_df['total_sales'] - min_sales) / (max_sales - min_sales + 1) * 50

    # Create the map with colorful markers
    fig_map = go.Figure(go.Scattergeo(
        locations=territory_df['territory'],
        locationmode='country names',
        marker=dict(
            size=territory_df['marker_size'],
            color=territory_df['total_sales'],
            colorscale='Plasma',  # More colorful gradient
            line=dict(width=1, color='white'),
            sizemode='area',
            opacity=0.85
        ),
        hoverinfo='text',
        hovertext=[
            f"<b>{row['territory']}</b><br>"
            f"Sales: ${row['total_sales']:,.0f}<br>"
            f"Customers: {row['customer_count']}<br>"
            f"Orders: {row['total_orders']}"
            for _, row in territory_df.iterrows()
        ],
        name=''
    ))

    fig_map.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=0, b=0),
        geo=dict(
            showframe=False,
            showcoastlines=True,
            coastlinecolor="rgba(203, 213, 225, 0.6)",
            showland=True,
            landcolor="rgba(241, 245, 249, 1)",
            showocean=True,
            oceancolor="rgba(226, 232, 240, 0.6)",
            showcountries=True,
            countrycolor="rgba(203, 213, 225, 1)",
            projection_type="natural earth",
            center=dict(lon=0, lat=20),
            projection_scale=1.2
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        showlegend=False
    )

    st.plotly_chart(fig_map, use_container_width=True)


