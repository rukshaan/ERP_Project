import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from utils.auth import require_auth
from utils.sidebar import render_sidebar

# =================== PAGE CONFIG ===================
st.set_page_config(layout="wide", page_title="💰 Sales Invoice Finance Dashboard")

# =================== AUTH GUARD ===================
require_auth()

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

# =================== DB CONNECTION (NO CACHE) ===================
def get_connection():
    return duckdb.connect("./data/Silver/dev.duckdb", read_only=False)

con = get_connection()

# =================== LOAD DATA ===================
query = """
SELECT
    sales_invoice_id,
    customer,
    posting_date,
    item_name,
    item_qty,
    item_amount,
    payment_amount,
    outstanding_amount,
    "due_date" AS due_date
FROM main_prod.fact_sales_invoice
"""
df = con.execute(query).df()

# =================== CLEAN DATA ===================
df.columns = df.columns.str.lower().str.replace(" ", "_")
df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce")
df["due_date"] = pd.to_datetime(
    df["due_date"].astype(str).str.replace('"', '').str.strip(),
    errors="coerce"
)

# =================== INVOICE LEVEL ===================
invoice_df = df.groupby("sales_invoice_id").agg({
    "customer": "first",
    "posting_date": "max",
    "item_amount": "sum",
    "payment_amount": "max",
    "outstanding_amount": "max",
    "due_date": "max"
}).reset_index()

# =================== DERIVED COLUMNS ===================
today = pd.Timestamp.today()

invoice_df["aging_days"] = (today - invoice_df["due_date"]).dt.days

invoice_df["status"] = invoice_df.apply(
    lambda x: "Paid" if x["outstanding_amount"] == 0
    else "Overdue" if pd.notnull(x["due_date"]) and x["due_date"] < today
    else "Unpaid",
    axis=1
)

invoice_df["payment_completion_%"] = (
    invoice_df["payment_amount"] / invoice_df["item_amount"]
).replace([float("inf"), -float("inf")], 0) * 100

# =================== STREAMLIT TABS ===================
tab1, tab2, tab3 = st.tabs([
    "Overview",
    "Customer Item Breakdown",
    "Sales Trends & Forecast"
])

# =================== TAB 1: OVERVIEW ===================
with tab1:
    st.header("📊 Overview KPIs")

    total_sales = invoice_df["item_amount"].sum()
    total_paid = invoice_df["payment_amount"].sum()
    total_unpaid = invoice_df["outstanding_amount"].sum()

    collection_rate = (total_paid / total_sales) * 100 if total_sales else 0
    num_customers = invoice_df["customer"].nunique()
    avg_invoice_value = invoice_df["item_amount"].mean()
    avg_payment_days = invoice_df["aging_days"].mean()

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Sales", f"₹{total_sales:,.0f}")
    k2.metric("Collected", f"₹{total_paid:,.0f}")
    k3.metric("Unpaid", f"₹{total_unpaid:,.0f}")
    k4.metric("Collection %", f"{collection_rate:.2f}%")
    k5.metric("Customers", f"{num_customers}")
    k6.metric("Avg Invoice Value", f"₹{avg_invoice_value:,.0f}")

    # Payment Distribution
    pie_df = pd.DataFrame({
        "Type": ["Paid", "Unpaid"],
        "Amount": [total_paid, total_unpaid]
    })

    st.plotly_chart(
        px.pie(pie_df, names="Type", values="Amount", title="Payment Distribution"),
        use_container_width=True
    )

    # Invoice Table
    st.header("📋 Invoice Table")
    st.dataframe(
        invoice_df.sort_values("posting_date", ascending=False),
        use_container_width=True
    )

    # Alerts
    st.subheader("⚠️ Alerts")
    if total_unpaid > 100000000:
        st.error("⚠️ High outstanding amount!")

    overdue_count = (invoice_df["status"] == "Overdue").sum()
    if overdue_count > 5:
        st.warning(f"⚠️ {overdue_count} overdue invoices!")

    # Aging Analysis
    st.header("⏳ Aging Analysis")

    def aging_bucket(days):
        if pd.isna(days):
            return "Unknown"
        elif days <= 0:
            return "Not Due"
        elif days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        else:
            return "90+"

    invoice_df["aging_bucket"] = invoice_df["aging_days"].apply(aging_bucket)

    aging_df = invoice_df.groupby("aging_bucket")["outstanding_amount"].sum().reset_index()

    st.plotly_chart(
        px.pie(
            aging_df,
            names="aging_bucket",
            values="outstanding_amount",
            title="Outstanding by Aging Bucket"
        ),
        use_container_width=True
    )

    # Revenue Trend
    st.header("📈 Revenue Trend")

    trend_df = invoice_df.groupby(
        invoice_df["posting_date"].dt.date
    ).agg({
        "item_amount": "sum",
        "payment_amount": "sum"
    }).reset_index()

    fig = px.line(
        trend_df,
        x="posting_date",
        y=["item_amount", "payment_amount"],
        title="Sales vs Collection Trend",
        markers=True
    )

    st.plotly_chart(fig, use_container_width=True)

# =================== TAB 2: CUSTOMER ITEM BREAKDOWN ===================
with tab2:
    st.header("👤 Customer Insights")

    customer_df = invoice_df.groupby("customer").agg({
        "item_amount": "sum",
        "payment_amount": "sum",
        "outstanding_amount": "sum"
    }).reset_index()

    customer_df["collection_%"] = (
        customer_df["payment_amount"] / customer_df["item_amount"]
    ).replace([float("inf"), -float("inf")], 0) * 100

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(
            px.bar(
                customer_df.sort_values("item_amount", ascending=False).head(10),
                x="customer", y="item_amount",
                title="Top Customers by Invoice Value"
            ),
            use_container_width=True
        )

    with col2:
        st.plotly_chart(
            px.bar(
                customer_df.sort_values("outstanding_amount", ascending=False).head(10),
                x="customer", y="outstanding_amount",
                title="Top Customers by Unpaid Amount"
            ),
            use_container_width=True
        )

    st.dataframe(
        customer_df.sort_values("outstanding_amount", ascending=False),
        use_container_width=True
    )

    st.header("📦 Item Insights")

    item_df = df.groupby("item_name").agg({
        "item_qty": "sum",
        "item_amount": "sum",
        "outstanding_amount": "sum"
    }).reset_index()

    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(
            px.bar(
                item_df.sort_values("item_amount", ascending=False).head(10),
                x="item_name", y="item_amount",
                title="Top Items by Revenue"
            ),
            use_container_width=True
        )

    with col2:
        st.plotly_chart(
            px.bar(
                item_df.sort_values("outstanding_amount", ascending=False).head(10),
                x="item_name", y="outstanding_amount",
                title="Items with Highest Outstanding"
            ),
            use_container_width=True
        )

    st.header("🛒 Customer Item Breakdown")

    cust_item_df = df.groupby(["customer", "item_name"]).agg({
        "item_qty": "sum",
        "item_amount": "sum",
        "outstanding_amount": "sum"
    }).reset_index()

    selected_customer = st.selectbox("Select Customer", cust_item_df["customer"].unique())

    customer_data = cust_item_df[cust_item_df["customer"] == selected_customer]

    st.subheader(f"Invoice Value Distribution for {selected_customer}")
    st.plotly_chart(
        px.pie(customer_data, names="item_name", values="item_amount", hole=0.3),
        use_container_width=True
    )

    st.subheader(f"Outstanding Amount Distribution for {selected_customer}")
    st.plotly_chart(
        px.pie(customer_data, names="item_name", values="outstanding_amount", hole=0.3),
        use_container_width=True
    )

    st.dataframe(customer_data.sort_values("item_amount", ascending=False), use_container_width=True)

# =================== TAB 3: SALES TRENDS & FORECAST ===================
with tab3:
    st.header("📈 Sales, Collections & Forecast")

    sales_time_df = invoice_df.groupby(
        invoice_df["posting_date"].dt.date
    ).agg({
        "item_amount": "sum",
        "payment_amount": "sum",
        "outstanding_amount": "sum"
    }).reset_index()

    st.plotly_chart(
        px.line(
            sales_time_df,
            x="posting_date",
            y=["item_amount", "payment_amount", "outstanding_amount"],
            markers=True
        ),
        use_container_width=True
    )

    # Forecast
    st.header("🔮 Forecast for Next 90 Days")

    forecast_df = sales_time_df.copy()
    forecast_df.set_index("posting_date", inplace=True)
    forecast_df = forecast_df.asfreq("D", fill_value=0)

    model = ExponentialSmoothing(
        forecast_df["item_amount"],
        trend="add",
        seasonal=None
    )

    model_fit = model.fit()
    forecast = model_fit.forecast(90)

    forecast_plot_df = forecast.reset_index()
    forecast_plot_df.columns = ["date", "forecasted_sales"]

    st.plotly_chart(
        px.line(forecast_plot_df, x="date", y="forecasted_sales", markers=True),
        use_container_width=True
    )

    # Customer-wise forecast
    customer_df = invoice_df.groupby("customer")["item_amount"].sum().reset_index()
    customer_df["proportion"] = customer_df["item_amount"] / customer_df["item_amount"].sum()

    customer_forecast = pd.DataFrame()

    for _, row in customer_df.iterrows():
        temp = pd.DataFrame({
            "customer": row["customer"],
            "date": forecast_plot_df["date"],
            "forecasted_sales": forecast_plot_df["forecasted_sales"] * row["proportion"]
        })
        customer_forecast = pd.concat([customer_forecast, temp], ignore_index=True)

    st.dataframe(customer_forecast.head(50), use_container_width=True)

    # Top items
    top_items_df = df.groupby("item_name")["item_amount"].sum().reset_index()
    top_items_df = top_items_df.sort_values("item_amount", ascending=False).head(10)

    st.dataframe(top_items_df, use_container_width=True)

    # Downloads
    st.download_button(
        "📥 Download Forecast CSV",
        forecast_plot_df.to_csv(index=False).encode("utf-8"),
        "sales_invoice_forecast.csv",
        "text/csv"
    )

    st.download_button(
        "📥 Download Customer-wise Forecast CSV",
        customer_forecast.to_csv(index=False).encode("utf-8"),
        "sales_invoice_customer_forecast.csv",
        "text/csv"
    )

# =================== CLOSE CONNECTION ===================
con.close()