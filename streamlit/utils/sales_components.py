import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from streamlit_plotly_events import plotly_events
from utils.prediction_utils import (
    predict_product_sales,
    predict_customers,
    customer_product_prediction
)
def render_sales_overview(con, filters):
    selected_customers = filters["customers"]
    selected_items = filters["items"]
    delivery_status = filters["delivery_status"]
    start_date = filters["start_date"]
    end_date = filters["end_date"]
    min_value = filters["min_value"]
    so_search = filters["so_search"]

    st.title("📈 Sales Order – Current Trends")

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

    st.subheader("📊 Month-to-Date Performance")
    k1, k2, k3 = st.columns(3)

    kpi_query = f"""
    SELECT
        SUM(f.amount) AS total_order_amount,
        COUNT(DISTINCT f.sales_order_id) AS total_orders,
        AVG(f.amount) AS avg_order_amount
    FROM main_prod.fact_sales_order f
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

    st.subheader("📈 Orders Over Time")
    granularity = st.radio("View By", ["Daily", "Monthly", "Yearly"], horizontal=True, key="so_granularity")
    date_col = ("f.order_date" if granularity == "Daily" else "date_trunc('month', f.order_date)" if granularity == "Monthly" else "date_trunc('year', f.order_date)")
    
    time_query = f"""
    SELECT
        {date_col} AS order_period,
        COUNT(DISTINCT f.sales_order_id) AS total_orders
    FROM main_prod.fact_sales_order f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    {build_where_clause()}
    GROUP BY order_period
    ORDER BY order_period
    """
    time_df = con.execute(time_query).df()
    fig_time = px.line(time_df, x="order_period", y="total_orders", markers=True, title="Orders Trend Over Time")
    st.plotly_chart(fig_time, use_container_width=True)

    st.subheader("🏆 Top Customers")
    customer_query = f"""
    SELECT c.customer_name, SUM(f.amount) AS total_value
    FROM main_prod.fact_sales_order f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    {build_where_clause()}
    GROUP BY c.customer_name
    ORDER BY total_value DESC
    LIMIT 10
    """
    customer_df = con.execute(customer_query).df()
    st.plotly_chart(px.bar(customer_df, x="customer_name", y="total_value", title="Top Customers by Sales Value"), use_container_width=True)

    st.subheader("📦 Top Items")
    item_query = f"""
    SELECT i.item_name, SUM(f.amount) AS total_value
    FROM main_prod.fact_sales_order f
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
        st.plotly_chart(px.bar(item_df, x="item_name", y="total_value", title="Top Items by Sales"), use_container_width=True)
    with c2:
        st.plotly_chart(px.pie(item_df, names="item_name", values="total_value", title="Item Contribution", hole=0.4), use_container_width=True)

    st.subheader("📋 Recent Orders")
    orders_query = f"""
    SELECT
        f.sales_order_id,
        c.customer_name,
        i.item_name,
        f.order_date,
        f.amount,
        f.order_status
    FROM main_prod.fact_sales_order f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    {build_where_clause()}
    ORDER BY f.order_date DESC
    """
    orders_df = con.execute(orders_query).df()
    st.dataframe(orders_df, use_container_width=True)

def render_backlog(con, filters):
    start_date = filters["start_date"]
    end_date = filters["end_date"]
    selected_customers = filters["customers"]
    selected_items = filters["items"]
    delivery_status = filters["delivery_status"]
    min_value = filters["min_value"]
    so_search = filters["so_search"]

    st.title("📋 Backlog / Open Orders Dashboard")

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

    st.header("📊 Key Performance Indicators")
    kpi_query = f"""
    SELECT 
        COALESCE(SUM(f.open_qty * f.rate), 0) AS total_open_value,
        COALESCE(COUNT(DISTINCT f.sales_order_id), 0) AS open_orders,
        COALESCE(AVG(DATEDIFF('day', f.order_date, f.delivery_date)), 0) AS avg_open_days,
        COALESCE(SUM(f.open_qty), 0) AS total_open_qty,
        COALESCE(COUNT(DISTINCT c.customer_name), 0) AS affected_customers,
        COALESCE(COUNT(DISTINCT i.item_name), 0) AS affected_items
    FROM main_prod.fact_sales_order f
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

    st.header("📈 Visual Analysis")
    tab1, tab2, tab3 = st.tabs(["Customer Analysis", "Item Analysis", "Trend Analysis"])
    with tab1:
        customer_chart_query = f"""
        SELECT 
            c.customer_name, 
            COALESCE(SUM(f.open_qty * f.rate), 0) AS open_value,
            COALESCE(SUM(f.open_qty), 0) AS open_qty,
            COUNT(DISTINCT f.sales_order_id) AS order_count
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        WHERE {where_clause}
        GROUP BY c.customer_name
        ORDER BY open_value DESC
        LIMIT 15
        """
        customer_chart_df = con.execute(customer_chart_query).df()
        if not customer_chart_df.empty:
            col1_chart, col2_chart = st.columns(2)
            with col1_chart:
                fig_cust_value = px.bar(customer_chart_df, x='customer_name', y='open_value', title="Top 15 Customers by Open Value", labels={'open_value':'Open Value (₹)','customer_name':'Customer'}, color='open_value', color_continuous_scale='Viridis')
                fig_cust_value.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_cust_value, use_container_width=True)
            with col2_chart:
                fig_cust_qty = px.pie(customer_chart_df.head(10), names='customer_name', values='open_qty', title="Customer Share by Open Quantity", hole=0.3)
                st.plotly_chart(fig_cust_qty, use_container_width=True)
        else:
            st.info("No customer data available.")

    with tab2:
        item_chart_query = f"""
        SELECT 
            i.item_name, 
            COALESCE(SUM(f.open_qty * f.rate), 0) AS open_value,
            COALESCE(SUM(f.open_qty), 0) AS open_qty,
            COUNT(DISTINCT f.sales_order_id) AS order_count
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        WHERE {where_clause}
        GROUP BY i.item_name
        ORDER BY open_value DESC
        LIMIT 15
        """
        item_chart_df = con.execute(item_chart_query).df()
        if not item_chart_df.empty:
            col1_chart, col2_chart = st.columns(2)
            with col1_chart:
                fig_item_value = px.bar(item_chart_df, x='item_name', y='open_value', title="Top 15 Items by Open Value", labels={'open_value':'Open Value (₹)','item_name':'Item'}, color='open_value', color_continuous_scale='Plasma')
                fig_item_value.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_item_value, use_container_width=True)
            with col2_chart:
                fig_item_qty = px.treemap(item_chart_df, path=['item_name'], values='open_qty', title="Item Distribution by Open Quantity", color='open_value', color_continuous_scale='Blues')
                st.plotly_chart(fig_item_qty, use_container_width=True)
        else:
            st.info("No item data available.")

    with tab3:
        st.subheader("Monthly Open Order Trend")
        trend_query = f"""
        SELECT 
            DATE_TRUNC('month', f.order_date) AS month,
            COALESCE(SUM(f.open_qty * f.rate), 0) AS monthly_open_value,
            COALESCE(SUM(f.open_qty), 0) AS monthly_open_qty,
            COUNT(DISTINCT f.sales_order_id) AS monthly_order_count
        FROM main_prod.fact_sales_order f
        JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
        JOIN main_prod.dim_item i ON f.item_code = i.item_code
        WHERE {where_clause}
        GROUP BY DATE_TRUNC('month', f.order_date)
        ORDER BY month
        """
        trend_df = con.execute(trend_query).df()
        if not trend_df.empty:
            trend_df['month'] = pd.to_datetime(trend_df['month']).dt.strftime('%b %Y')
            trend_df['monthly_open_value_fmt'] = trend_df['monthly_open_value'].map(lambda x: f"₹{x:,.2f}")
            display_df = trend_df.rename(columns={'month': 'Month', 'monthly_open_value_fmt': 'Open Value (₹)', 'monthly_open_qty': 'Open Quantity', 'monthly_order_count': 'Order Count'})[['Month', 'Open Value (₹)', 'Open Quantity', 'Order Count']]
            st.markdown("### 📋 Monthly Trend Table")
            st.dataframe(display_df, use_container_width=True)
            fig_trend = px.line(trend_df, x='month', y='monthly_open_value', title="Open Order Value Trend by Month", labels={'monthly_open_value': 'Open Value (₹)', 'month': 'Month'}, markers=True)
            fig_trend.add_bar(x=trend_df['month'], y=trend_df['monthly_order_count'], name='Order Count', yaxis='y2', opacity=0.3)
            fig_trend.update_layout(yaxis2=dict(title='Order Count', overlaying='y', side='right'), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("No trend data available.")

    st.header("📋 Detailed Open Orders")
    sort_options = {
        "Delivery Date (Ascending)": "f.delivery_date ASC",
        "Delivery Date (Descending)": "f.delivery_date DESC",
        "Days Until Delivery (Soonest First)": "days_until_delivery ASC",
        "Days Until Delivery (Latest First)": "days_until_delivery DESC",
        "Order Value (High to Low)": "pending_amount DESC",
        "Order Value (Low to High)": "pending_amount ASC",
        "Pending Qty (High to Low)": "f.open_qty DESC",
        "Pending Qty (Low to High)": "f.open_qty ASC",
        "Customer Name (A → Z)": "c.customer_name ASC",
        "Customer Name (Z → A)": "c.customer_name DESC",
        "Order Date (Newest First)": "f.order_date DESC",
        "Order Date (Oldest First)": "f.order_date ASC",
        "Sales Order ID": "f.sales_order_id ASC",
        "Delivery Status (Overdue → Urgent → On Track)": "CASE WHEN f.delivery_date < CURRENT_DATE THEN 1 WHEN f.delivery_date <= DATE_ADD(CURRENT_DATE, 3) THEN 2 ELSE 3 END ASC",
        "Priority View (Status → Delivery → Value)": "CASE WHEN f.delivery_date < CURRENT_DATE THEN 1 WHEN f.delivery_date <= DATE_ADD(CURRENT_DATE, 3) THEN 2 ELSE 3 END, f.delivery_date ASC, pending_amount DESC"
    }
    sort_by = st.selectbox("Sort Table By:", options=list(sort_options.keys()), index=0, key="backlog_sort")
    table_query = f"""
    SELECT f.sales_order_id, 
        c.customer_name, 
        i.item_name,
        f.qty AS ordered_qty, 
        f.open_qty AS pending_qty, 
        f.rate,
        (f.open_qty * f.rate) AS pending_amount,
        f.order_date, 
        f.delivery_date,
        DATEDIFF('day', CURRENT_DATE, f.delivery_date) AS days_until_delivery,
        CASE 
            WHEN f.delivery_date < CURRENT_DATE THEN 'Overdue'
            WHEN f.delivery_date <= DATE_ADD(CURRENT_DATE, 3) THEN 'Urgent'
            ELSE 'On Track'
        END AS delivery_status
    FROM main_prod.fact_sales_order f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    WHERE {where_clause}
    ORDER BY {sort_options[sort_by]}
    """
    open_orders_df = con.execute(table_query).df()
    if not open_orders_df.empty:
        display_df = open_orders_df.copy()
        display_df['rate'] = display_df['rate'].apply(lambda x: f"₹{x:,.2f}")
        display_df['pending_amount'] = display_df['pending_amount'].apply(lambda x: f"₹{x:,.2f}")
        display_df['order_date'] = pd.to_datetime(display_df['order_date']).dt.strftime('%Y-%m-%d')
        display_df['delivery_date'] = pd.to_datetime(display_df['delivery_date']).dt.strftime('%Y-%m-%d')
        def color_status(val):
            if val == 'Overdue': return 'background-color: #ffcccc; color: #cc0000'
            elif val == 'Urgent': return 'background-color: #fff3cd; color: #856404'
            else: return 'background-color: #d4edda; color: #155724'
        styled_df = display_df.style.applymap(color_status, subset=['delivery_status'])
        st.dataframe(styled_df, use_container_width=True, height=400)
    else:
        st.success("🎉 No open orders found with the current filters!")

    base_query = f"""
    SELECT
        f.sales_order_id,
        f.order_date,
        f.delivery_date,
        f.order_status,
        f.qty,
        f.open_qty,
        f.rate,
        (f.open_qty * f.rate) AS amount,
        c.customer_name,
        i.item_name
    FROM main_prod.fact_sales_order f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    WHERE {where_clause}
    """
    df = con.execute(base_query).df()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["delivery_date"] = pd.to_datetime(df["delivery_date"])

    st.subheader("📊 Order Status Distribution")
    status_counts = df.groupby("order_status")["sales_order_id"].nunique().reset_index(name="num_orders")
    if not status_counts.empty:
        fig_status = px.bar(status_counts, x="order_status", y="num_orders", text="num_orders", title="Number of Unique Sales Orders by Order Status")
        fig_status.update_layout(xaxis_title="Order Status", yaxis_title="Number of Orders", showlegend=False)
        st.plotly_chart(fig_status, use_container_width=True)
    else:
        st.info("No order status data for selected filters.")

    st.subheader("📈 Sales Orders Over Time by Status")
    order_statuses = sorted(df["order_status"].dropna().unique().tolist())
    selected_status = st.selectbox("Select Order Status", ["All"] + order_statuses, key="order_status_filter")
    if selected_status == "All":
        df_filtered = df.copy()
        title = "Daily Orders for All Statuses"
    else:
        df_filtered = df[df["order_status"] == selected_status].copy()
        title = f"Daily Orders for - {selected_status}"
    
    agg = df_filtered.groupby(["order_date", "order_status"]).agg(num_orders=("sales_order_id", "nunique")).reset_index()
    selected_points = []
    if agg.empty:
        st.info("No trend data available.")
    else:
        fig = px.line(agg, x="order_date", y="num_orders", color="order_status" if selected_status == "All" else None, markers=True, title=title)
        fig.update_layout(xaxis_title="Order Date", yaxis_title="Number of Orders", hovermode="closest")
        st.plotly_chart(fig, use_container_width=True)
        selected_points = plotly_events(fig, click_event=True, hover_event=False, select_event=False, key="orders_trend_click")

    orders_table_df = df_filtered.sort_values("order_date").drop_duplicates(subset=["sales_order_id"])
    order_count = orders_table_df["sales_order_id"].nunique()
    status_label = "All Statuses" if selected_status == "All" else selected_status
    st.subheader(f"📋 Order Details ({status_label}) - {order_count} Orders")
    st.dataframe(orders_table_df[["sales_order_id", "order_date", "order_status", "customer_name", "item_name", "amount"]], use_container_width=True)

    if selected_points:
        clicked = selected_points[0]
        clicked_date = pd.to_datetime(clicked["x"]).date()
        if selected_status == "All":
            status = agg["order_status"].unique()[clicked["curveNumber"]]
        else:
            status = selected_status
        drill_df = df_filtered[(df_filtered["order_date"].dt.date == clicked_date) & (df_filtered["order_status"] == status)].drop_duplicates(subset=["sales_order_id"])
        st.markdown("### 🔍 Drill-down: Clicked Point Orders")
        st.success(f"📅 Date: {clicked_date} | 🏷 Status: {status} | 📦 Orders: {drill_df.shape[0]}")
        st.dataframe(drill_df[["sales_order_id", "order_date", "order_status", "customer_name", "item_name", "amount"]], use_container_width=True)

    if not open_orders_df.empty:
        st.header("📊 Summary Statistics")
        summary_cols = st.columns(4)
        with summary_cols[0]:
            st.metric("Overdue Orders", len(open_orders_df[open_orders_df['delivery_status'] == 'Overdue']))
        with summary_cols[1]:
            st.metric("Urgent Orders", len(open_orders_df[open_orders_df['delivery_status'] == 'Urgent']))
        with summary_cols[2]:
            st.metric("Avg Days to Delivery", f"{open_orders_df['days_until_delivery'].mean():.1f}")
        with summary_cols[3]:
            st.metric("Largest Order", f"₹{open_orders_df['pending_amount'].max():,.0f}")

def render_delivery_insights(con, filters):
    st.title("🚚 Delivery Performance & Insights")

    def build_filter_clause(filters: dict):
        clauses = ["f.open_qty > 0"]
        clauses.append(f"f.order_date BETWEEN '{filters['start_date']}' AND '{filters['end_date']}'")
        if filters["customers"]:
            customers = "', '".join(filters["customers"])
            clauses.append(f"f.customer_name IN ('{customers}')")
        if filters["items"]:
            items = "', '".join(filters["items"])
            clauses.append(f"f.item_code IN (SELECT item_code FROM main_prod.dim_item WHERE item_name IN ('{items}'))")
        today = datetime.now().date()
        if filters["delivery_status"] == "On Time":
            clauses.append("f.delivery_date <= f.order_date")
        elif filters["delivery_status"] == "Overdue":
            clauses.append("f.delivery_date > f.order_date")
        elif filters["delivery_status"] == "Upcoming":
            clauses.append(f"f.delivery_date > '{today}'")
        elif filters["delivery_status"] == "Urgent (< 3 days)":
            clauses.append(f"DATEDIFF('day', '{today}', f.delivery_date) <= 3")
        if filters["min_value"] > 0:
            clauses.append(f"f.amount >= {filters['min_value']}")
        if filters["so_search"]:
            clauses.append(f"f.sales_order_id LIKE '%{filters['so_search']}%'")
        return " AND ".join(clauses)

    where_clause = build_filter_clause(filters)

    kpi_query = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN f.delivery_date <= f.order_date THEN f.sales_order_id END) AS on_time,
        COUNT(DISTINCT CASE WHEN f.delivery_date > f.order_date THEN f.sales_order_id END) AS late,
        AVG(DATEDIFF('day', f.order_date, f.delivery_date)) AS avg_delay
    FROM main_prod.fact_sales_order f
    WHERE {where_clause}
    """
    kpi_df = con.execute(kpi_query).df()

    c1, c2, c3 = st.columns(3)
    c1.metric("On-Time Orders", int(kpi_df.iloc[0]["on_time"]))
    c2.metric("Late Orders", int(kpi_df.iloc[0]["late"]))
    c3.metric("Average Delay (days)", round(kpi_df.iloc[0]["avg_delay"], 1))

    chart_df = pd.DataFrame({"Status": ["On-Time", "Late"], "Orders": [kpi_df.iloc[0]["on_time"], kpi_df.iloc[0]["late"]]})
    st.plotly_chart(px.pie(chart_df, names="Status", values="Orders", title="Delivery Performance"), use_container_width=True)

    st.subheader("🏆 Top Customers")
    customer_df = con.execute(f"SELECT customer_name, SUM(amount) AS total_value FROM main_prod.fact_sales_order f WHERE {where_clause} GROUP BY customer_name ORDER BY total_value DESC LIMIT 10").df()
    if customer_df.empty:
        st.info("No customer data available.")
    else:
        st.plotly_chart(px.bar(customer_df, x="customer_name", y="total_value", title="Top Customers by Order Value"), use_container_width=True)

    st.subheader("📦 Customer Item Breakdown")
    selected_customer = st.selectbox("Select a customer", customer_df["customer_name"].unique(), key="delivery_customer")
    item_df = con.execute(f"SELECT i.item_name, SUM(f.qty) AS total_qty, SUM(f.amount) AS total_value FROM main_prod.fact_sales_order f JOIN main_prod.dim_item i ON f.item_code = i.item_code WHERE {where_clause} AND f.customer_name = '{selected_customer}' GROUP BY i.item_name ORDER BY total_qty DESC").df()
    if not item_df.empty:
        fig = px.pie(item_df, names="item_name", values="total_qty", hole=0.3, title=f"Item Quantity Share – {selected_customer}")
        fig.update_traces(textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No item data available.")

    st.subheader("🔥 Top Items by Quantity")
    top_items = con.execute(f"SELECT i.item_name, SUM(f.qty) AS total_qty FROM main_prod.fact_sales_order f JOIN main_prod.dim_item i ON f.item_code = i.item_code WHERE {where_clause} GROUP BY i.item_name ORDER BY total_qty DESC LIMIT 10").df()
    if not top_items.empty:
        st.plotly_chart(px.bar(top_items, x="item_name", y="total_qty", title="Top Items by Quantity"), use_container_width=True)

def render_sales_order_forecast(con, filters):
    import sys
    import os
    import duckdb

    # Keep your original import (but correct path handling)
    from utils.prediction_utils import (
        predict_product_sales,
        predict_customers,
        customer_product_prediction
    )

    

    # ---------------------- TITLE ----------------------
    st.title("📊 Sales & Customer Forecasts")

    # ---------------------- FILTERS ----------------------
    # Use passed filters FIRST, fallback to sidebar if missing
    if filters is None:
        try:
            filters = render_sidebar()
        except:
            st.error("Filters not provided.")
            st.stop()

    start_date = filters["start_date"]
    end_date = filters["end_date"]
    selected_customers = filters["customers"]
    selected_items = filters["items"]

    # ---------------------- DB ----------------------
    # Use passed connection; fallback if not provided
    if con is None:
        DB_PATH = "./data/Silver/dev.duckdb"
        con = duckdb.connect(DB_PATH, read_only=True)

    # ---------------------- BASE QUERY ----------------------
    where_conditions = [f"f.order_date BETWEEN '{start_date}' AND '{end_date}'"]

    if selected_customers:
        cust_str = "', '".join(selected_customers)
        where_conditions.append(f"c.customer_name IN ('{cust_str}')")

    if selected_items:
        item_str = "', '".join(selected_items)
        where_conditions.append(f"i.item_name IN ('{item_str}')")

    where_clause = " AND ".join(where_conditions)

    query = f"""
    SELECT
        f.sales_order_id,
        f.order_date,
        f.delivery_date,
        f.qty,
        f.open_qty,
        f.rate,
        c.customer_name,
        i.item_name
    FROM main_prod.fact_sales_order f
    JOIN main_prod.dim_customer c ON f.customer_name = c.customer_name
    JOIN main_prod.dim_item i ON f.item_code = i.item_code
    WHERE {where_clause}
    """

    df = con.execute(query).df()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["delivery_date"] = pd.to_datetime(df["delivery_date"])

    if df.empty:
        st.warning("No data available for selected filters.")
        st.stop()

    # ---------------------- HISTORICAL SALES ----------------------
    st.header("📅 Historical Daily Sales")

    df_hist = (
        df.groupby(["order_date", "item_name"], as_index=False)["qty"]
        .sum()
        .rename(columns={"order_date": "date", "qty": "daily_units"})
    )

    st.dataframe(df_hist, use_container_width=True)

    csv_hist = df_hist.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Historical Sales CSV",
        csv_hist,
        f"historical_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "text/csv"
    )

    # ---------------------- PRODUCT FORECAST ----------------------
    st.header("📦 Product Forecast (Next 30 Days)")

    forecast_days = st.slider(
        "Forecast Horizon (days)",
        min_value=7,
        max_value=90,
        value=30
    )

    product_forecast_df = predict_product_sales(df, forecast_days)
    future_forecast = product_forecast_df[
        product_forecast_df["date"] > df_hist["date"].max()
    ]

    st.dataframe(future_forecast, use_container_width=True)

    csv_forecast = future_forecast.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Forecast CSV",
        csv_forecast,
        f"product_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "text/csv"
    )

    # ---------------------- TOP ITEMS ----------------------
    st.subheader("Top 15 Items by Forecasted Units")
    top_items_forecast = (
        future_forecast.groupby('item_name')['predicted_units']
        .sum()
        .sort_values(ascending=False)
        .head(15)
    )
    st.bar_chart(top_items_forecast)

    # ---------------------- CUSTOMER FORECAST ----------------------
    st.header("👤 Likely Active Customers")

    customer_forecast_df = predict_customers(df)
    st.dataframe(customer_forecast_df, use_container_width=True)

    csv_cust = customer_forecast_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Customer Forecast CSV",
        csv_cust,
        f"customer_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "text/csv"
    )

    # ---------------------- CUSTOMER → ITEM FORECAST ----------------------
    st.header("📋 Predicted Items per Customer")

    customer_product_df = customer_product_prediction(df, forecast_days=forecast_days)

    customer_product_df['predicted_date'] = pd.to_datetime(
        customer_product_df['predicted_date']
    ).dt.date

    customer_product_df = customer_product_df.sort_values(
        ["predicted_date", "customer_name", "predicted_units"],
        ascending=[True, True, False]
    )

    # ---------------------- CUSTOMER FILTER ----------------------
    st.subheader("🔍 Filter by Customer")

    customer_list = ["All"] + sorted(
        customer_product_df["customer_name"].unique().tolist()
    )

    selected_customer = st.selectbox("Select Customer", customer_list)

    if selected_customer != "All":
        customer_product_df = customer_product_df[
            customer_product_df["customer_name"] == selected_customer
        ]

    # ---------------------- DISPLAY ----------------------
    st.dataframe(customer_product_df, use_container_width=True)

    csv_cust_prod = customer_product_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Customer-Item Forecast CSV",
        csv_cust_prod,
        f"customer_item_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "text/csv"
    )

    # ---------------------- FOOTER ----------------------
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")