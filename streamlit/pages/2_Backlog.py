import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime
from utils.sidebar import render_sidebar  # Your global sidebar

st.set_page_config(layout="wide")
# =================== DB CONNECTION ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=False)

# =================== PAGE TITLE ===================
st.title("📋 Backlog / Open Orders Dashboard")

# =================== LOAD FILTERS ===================
filters = render_sidebar()
start_date = filters["start_date"]
end_date = filters["end_date"]
selected_customers = filters["customers"]
selected_items = filters["items"]
delivery_status = filters["delivery_status"]
min_value = filters["min_value"]
so_search = filters["so_search"]

# =================== BUILD WHERE CLAUSE ===================
filter_conditions = ["f.open_qty > 0"]

filter_conditions.append(f"f.order_date BETWEEN '{start_date}' AND '{end_date}'")

if selected_customers:
    customers_str = "', '".join(selected_customers)
    filter_conditions.append(f"c.customer_name IN ('{customers_str}')")

if selected_items:
    items_str = "', '".join(selected_items)
    filter_conditions.append(f"i.item_name IN ('{items_str}')")

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

# =================== KPI SECTION ===================
st.header("📊 Key Performance Indicators")

kpi_query = f"""
SELECT 
    COALESCE(SUM(f.open_qty * f.rate), 0) AS total_open_value,
    COALESCE(COUNT(DISTINCT f.sales_order_id), 0) AS open_orders,
    COALESCE(AVG(DATEDIFF('day', f.order_date, f.delivery_date)), 0) AS avg_open_days,
    COALESCE(SUM(f.open_qty), 0) AS total_open_qty,
    COALESCE(COUNT(DISTINCT c.customer_name), 0) AS affected_customers,
    COALESCE(COUNT(DISTINCT i.item_name), 0) AS affected_items
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE {where_clause}
"""
kpi_df = con.execute(kpi_query).df()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Open Value", f"₹{kpi_df['total_open_value'][0]:,.0f}")
    st.metric("Total Open Qty", f"{kpi_df['total_open_qty'][0]:,.0f}")
with col2:
    st.metric("Open Orders", int(kpi_df['open_orders'][0]))
    st.metric("Affected Customers", int(kpi_df['affected_customers'][0]))
with col3:
    st.metric("Avg Open Days", f"{kpi_df['avg_open_days'][0]:.1f}")
    st.metric("Affected Items", int(kpi_df['affected_items'][0]))

st.caption(f"📋 **Applied Filters:** {len(selected_customers)} customers, {len(selected_items)} items | 📅 {start_date} to {end_date} | 🚚 {delivery_status}")

# =================== CHARTS SECTION ===================
st.header("📈 Visual Analysis")
tab1, tab2, tab3 = st.tabs(["Customer Analysis", "Item Analysis", "Trend Analysis"])

# ------------------- Customer Analysis -------------------
with tab1:
    customer_chart_query = f"""
    SELECT 
        c.customer_name, 
        COALESCE(SUM(f.open_qty * f.rate), 0) AS open_value,
        COALESCE(SUM(f.open_qty), 0) AS open_qty,
        COUNT(DISTINCT f.sales_order_id) AS order_count
    FROM main_prod.fact_final_joined_files f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    WHERE {where_clause}
    GROUP BY c.customer_name
    ORDER BY open_value DESC
    LIMIT 15
    """
    customer_chart_df = con.execute(customer_chart_query).df()
    if not customer_chart_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig_cust_value = px.bar(
                customer_chart_df, x='customer_name', y='open_value',
                title="Top 15 Customers by Open Value",
                labels={'open_value':'Open Value (₹)','customer_name':'Customer'},
                color='open_value', color_continuous_scale='Viridis'
            )
            fig_cust_value.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_cust_value, use_container_width=True)
        with col2:
            fig_cust_qty = px.pie(
                customer_chart_df.head(10), names='customer_name', values='open_qty',
                title="Customer Share by Open Quantity", hole=0.3
            )
            st.plotly_chart(fig_cust_qty, use_container_width=True)
    else:
        st.info("No customer data available with current filters.")

# ------------------- Item Analysis -------------------
with tab2:
    item_chart_query = f"""
    SELECT 
        i.item_name, 
        COALESCE(SUM(f.open_qty * f.rate), 0) AS open_value,
        COALESCE(SUM(f.open_qty), 0) AS open_qty,
        COUNT(DISTINCT f.sales_order_id) AS order_count
    FROM main_prod.fact_final_joined_files f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    WHERE {where_clause}
    GROUP BY i.item_name
    ORDER BY open_value DESC
    LIMIT 15
    """
    item_chart_df = con.execute(item_chart_query).df()
    if not item_chart_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig_item_value = px.bar(
                item_chart_df, x='item_name', y='open_value',
                title="Top 15 Items by Open Value",
                labels={'open_value':'Open Value (₹)','item_name':'Item'},
                color='open_value', color_continuous_scale='Plasma'
            )
            fig_item_value.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_item_value, use_container_width=True)
        with col2:
            fig_item_qty = px.treemap(
                item_chart_df, path=['item_name'], values='open_qty',
                title="Item Distribution by Open Quantity",
                color='open_value', color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_item_qty, use_container_width=True)
    else:
        st.info("No item data available with current filters.")

# ------------------- Trend Analysis -------------------
with tab3:
    trend_query = f"""
    SELECT DATE_TRUNC('month', f.order_date) AS month,
           COALESCE(SUM(f.open_qty * f.rate), 0) AS monthly_open_value,
           COALESCE(SUM(f.open_qty), 0) AS monthly_open_qty,
           COUNT(DISTINCT f.sales_order_id) AS monthly_order_count
    FROM main_prod.fact_final_joined_files f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    WHERE {where_clause}
    GROUP BY DATE_TRUNC('month', f.order_date)
    ORDER BY month
    """
    trend_df = con.execute(trend_query).df()
    if not trend_df.empty:
        trend_df['month'] = pd.to_datetime(trend_df['month']).dt.strftime('%b %Y')
        fig_trend = px.line(
            trend_df, x='month', y='monthly_open_value',
            title="Open Order Value Trend by Month",
            labels={'monthly_open_value':'Open Value (₹)','month':'Month'},
            markers=True
        )
        fig_trend.add_bar(
            x=trend_df['month'], y=trend_df['monthly_order_count'],
            name='Order Count', yaxis='y2', opacity=0.3
        )
        fig_trend.update_layout(yaxis2=dict(title='Order Count', overlaying='y', side='right'))
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No trend data available with current filters.")

# =================== DETAILED TABLE ===================
st.header("📋 Detailed Open Orders")

sort_options = {
    "Delivery Date (Ascending)": "f.delivery_date ASC",
    "Delivery Date (Descending)": "f.delivery_date DESC",
    "Order Value (High to Low)": "pending_amount DESC",
    "Order Value (Low to High)": "pending_amount ASC",
    "Customer Name": "c.customer_name ASC"
}
sort_by = st.selectbox("Sort Table By:", options=list(sort_options.keys()), index=0)

table_query = f"""
SELECT f.sales_order_id, c.customer_name, i.item_name,
       f.qty AS ordered_qty, f.open_qty AS pending_qty, f.rate,
       (f.open_qty * f.rate) AS pending_amount,
       f.order_date, f.delivery_date,
       DATEDIFF('day', CURRENT_DATE, f.delivery_date) AS days_until_delivery,
       CASE 
         WHEN f.delivery_date < CURRENT_DATE THEN 'Overdue'
         WHEN f.delivery_date <= DATE_ADD(CURRENT_DATE, 3) THEN 'Urgent'
         ELSE 'On Track'
       END AS delivery_status
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE {where_clause}
ORDER BY {sort_options[sort_by]}
LIMIT 500
"""
table_df = con.execute(table_query).df()

if not table_df.empty:
    display_df = table_df.copy()
    display_df['rate'] = display_df['rate'].apply(lambda x: f"₹{x:,.2f}")
    display_df['pending_amount'] = display_df['pending_amount'].apply(lambda x: f"₹{x:,.2f}")
    display_df['order_date'] = pd.to_datetime(display_df['order_date']).dt.strftime('%Y-%m-%d')
    display_df['delivery_date'] = pd.to_datetime(display_df['delivery_date']).dt.strftime('%Y-%m-%d')

    def color_status(val):
        if val == 'Overdue': return 'background-color: #ffcccc; color: #cc0000'
        elif val == 'Urgent': return 'background-color: #fff3cd; color: #856404'
        else: return 'background-color: #d4edda; color: #155724'

    styled_df = display_df.style.applymap(color_status, subset=['delivery_status'])
    st.metric("Total Records Displayed", len(display_df))
    st.dataframe(styled_df, use_container_width=True, height=400)

    csv = table_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv,
        file_name=f"open_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
else:
    st.success("🎉 No open orders found with the current filters!")

# =================== SUMMARY STATISTICS ===================
if not table_df.empty:
    st.header("📊 Summary Statistics")
    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("Overdue Orders", len(table_df[table_df['delivery_status'] == 'Overdue']))
    with summary_cols[1]:
        st.metric("Urgent Orders", len(table_df[table_df['delivery_status'] == 'Urgent']))
    with summary_cols[2]:
        st.metric("Avg Days to Delivery", f"{table_df['days_until_delivery'].mean():.1f}")
    with summary_cols[3]:
        st.metric("Largest Order", f"₹{table_df['pending_amount'].max():,.0f}")

# =================== FOOTER ===================
con.close()
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Filters applied to all sections above")
