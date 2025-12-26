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

# ==================================================
# 🌍 CUSTOMER SALES – WORLD MAP VISUALIZATION
# ==================================================

st.subheader("🌍 Customer Sales – World Map View")

# Query for customer sales data
map_query = """
SELECT
    c.customer_name,
    c.territory,
    SUM(f.amount) AS total_sales,
    COUNT(DISTINCT f.sales_order_id) AS total_orders,
    AVG(f.amount) AS avg_order_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c
    ON f.customer_name = c.customer_name
GROUP BY c.customer_name, c.territory
HAVING SUM(f.amount) > 0
"""

map_df = con.execute(map_query).df()

if map_df.empty:
    st.warning("No data available for world map.")
else:
    # Create a container with custom styling
    with st.container():
        st.markdown("""
        <style>
        .map-container {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 15px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        .map-title {
            color: white;
            font-size: 24px;
            font-weight: 600;
            margin-bottom: 10px;
        }
        .map-subtitle {
            color: rgba(255,255,255,0.8);
            font-size: 14px;
            margin-bottom: 20px;
        }
        </style>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="map-container">', unsafe_allow_html=True)
        st.markdown('<div class="map-title">Customer Sales Distribution</div>', unsafe_allow_html=True)
        st.markdown('<div class="map-subtitle">Global overview of sales performance across territories</div>', unsafe_allow_html=True)
    
    # Create two columns for map and metrics
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Create the world map
        fig_map = px.scatter_geo(
            map_df,
            locations="territory",
            locationmode="country names",
            size="total_sales",
            color="total_sales",
            hover_name="customer_name",
            size_max=35,
            projection="natural earth",
            color_continuous_scale=px.colors.sequential.Viridis,
            hover_data={
                "territory": True,
                "total_sales": ":$,.2f",
                "total_orders": True,
                "avg_order_value": ":$,.2f"
            },
            custom_data=["customer_name", "territory", "total_sales", "total_orders", "avg_order_value"]
        )
        
        # Customize hover template
        fig_map.update_traces(
            hovertemplate="""
            <div style='background: white; padding: 10px; border-radius: 5px; border: 1px solid #e0e0e0'>
                <span style='font-size: 16px; font-weight: bold; color: #667eea'>%{customdata[0]}</span><br>
                <span style='color: #666'>📍 %{customdata[1]}</span><br><br>
                <span style='font-weight: bold'>Total Sales:</span> $%{customdata[2]:,.2f}<br>
                <span style='font-weight: bold'>Total Orders:</span> %{customdata[3]}<br>
                <span style='font-weight: bold'>Avg Order Value:</span> $%{customdata[4]:,.2f}
            </div>
            <extra></extra>
            """,
            marker=dict(
                line=dict(width=1, color='white'),
                sizemode='diameter'
            )
        )
        
        # Enhanced layout with clean design
        fig_map.update_layout(
            height=500,
            margin=dict(l=0, r=0, t=30, b=0),
            geo=dict(
                showframe=False,
                showcoastlines=True,
                coastlinecolor="lightgray",
                showland=True,
                landcolor="rgba(245, 245, 245, 0.8)",
                showocean=True,
                oceancolor="rgba(173, 216, 230, 0.3)",
                showcountries=True,
                countrycolor="white",
                countrywidth=0.5,
                bgcolor='rgba(255, 255, 255, 0.0)',
                projection_type="natural earth"
            ),
            coloraxis_colorbar=dict(
                title="Sales ($)",
                thickness=20,
                len=0.75,
                yanchor="middle",
                y=0.5,
                xanchor="right",
                x=1.05,
                tickformat="$,.0f",
                title_font=dict(size=12),
                tickfont=dict(size=10)
            ),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            title=dict(
                text="",
                font=dict(size=18, color="#2c3e50"),
                x=0.5,
                xanchor="center"
            )
        )
        
        st.plotly_chart(fig_map, use_container_width=True)
    
    with col2:
        # Display key metrics in a clean sidebar style
        st.markdown("""
        <style>
        .metric-card {
            background: white;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            border-left: 4px solid #667eea;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
            color: #2c3e50;
            line-height: 1.2;
        }
        .metric-label {
            font-size: 12px;
            color: #7f8c8d;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .country-badge {
            display: inline-block;
            background: #f8f9fa;
            padding: 5px 10px;
            border-radius: 15px;
            margin: 3px;
            font-size: 11px;
            color: #495057;
            border: 1px solid #dee2e6;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Top metrics
        total_sales = map_df['total_sales'].sum()
        avg_sales = map_df['total_sales'].mean()
        unique_countries = map_df['territory'].nunique()
        top_country = map_df.loc[map_df['total_sales'].idxmax(), 'territory']
        top_country_sales = map_df['total_sales'].max()
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">TOTAL SALES</div>
            <div class="metric-value">${total_sales:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">COUNTRIES</div>
            <div class="metric-value">{unique_countries}</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">TOP COUNTRY</div>
            <div class="metric-value">{top_country}</div>
            <div class="metric-label">${top_country_sales:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">AVG SALES PER CUSTOMER</div>
            <div class="metric-value">${avg_sales:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Top countries list
        st.markdown("**Top 5 Countries**")
        top_countries = map_df.groupby('territory')['total_sales'].sum().nlargest(5)
        for country, sales in top_countries.items():
            st.markdown(f"""
            <div style='padding: 8px 0; border-bottom: 1px solid #eee'>
                <div style='font-size: 14px; font-weight: 500'>{country}</div>
                <div style='font-size: 12px; color: #667eea'>${sales:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # Close the container
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Add some insights below the map
    with st.expander("📊 Map Insights"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Customers", len(map_df))
        with col2:
            st.metric("Average Orders per Customer", 
                     f"{map_df['total_orders'].mean():.1f}")
        with col3:
            st.metric("Sales Concentration",
                     f"{(top_country_sales / total_sales * 100):.1f}%")