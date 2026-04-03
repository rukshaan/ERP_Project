import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from utils.auth import require_auth
from utils.sidebar import render_sidebar

# =================== PAGE CONFIG ===================
st.set_page_config(layout="wide", page_title="📑 Sales Quotation Dashboard")

# =================== AUTH ===================
require_auth()

# =================== SIDEBAR ===================
filters = render_sidebar()
if filters is None:
    st.stop()

selected_customers = filters.get("customers", [])
start_date = filters.get("start_date", None)
end_date = filters.get("end_date", None)
min_value = filters.get("min_value", None)
so_search = filters.get("so_search", None)

# =================== DB CONNECTION ===================
if "db_conn" not in st.session_state:
    st.session_state.db_conn = duckdb.connect(
        "./data/Silver/dev.duckdb",
        read_only=True
    )

con = st.session_state.db_conn

# =================== LOAD DATA ===================
@st.cache_data
def load_data():
    query = """
    SELECT
        quotation_id,
        customer_name,
        transaction_date_key,
        valid_till_date_key,
        net_total,
        total_qty,
        item_name,
        item_code,
        status,
        currency
    FROM main_prod.fact_quotation
    """
    df = con.execute(query).df()
    return df

df = load_data()

# =================== DEBUG RAW DATA ===================
st.write("### Raw Data Preview")
st.dataframe(df.head(10))
st.write("Total rows in raw data:", len(df))
st.write("Column types:", df.dtypes)

# =================== CLEAN AND DATE CONVERSION ===================
df.columns = df.columns.str.lower()

# Detect the date format in your data
# Example formats: 'DD-MM-YYYY', 'DD/MM/YYYY', 'YYYY-MM-DD'
date_format = "%d-%m-%Y"  # update this if your dates are different

df["transaction_date"] = pd.to_datetime(df["transaction_date_key"], format=date_format, errors="coerce")
df["valid_till"] = pd.to_datetime(df["valid_till_date_key"], format=date_format, errors="coerce")

# Drop rows with invalid transaction_date
df = df[df["transaction_date"].notna()].copy()

st.write("### After Date Conversion")
st.dataframe(df.head(10))
st.write("Rows after dropping invalid dates:", len(df))

# =================== QUOTATION LEVEL AGGREGATION ===================
quote_df = df.groupby("quotation_id").agg({
    "customer_name": "first",
    "transaction_date": "max",
    "valid_till": "max",
    "net_total": "sum",
    "total_qty": "sum",
    "status": "first"
}).reset_index()

# =================== DERIVED FIELDS ===================
today = pd.Timestamp.today()
quote_df["days_to_expiry"] = (quote_df["valid_till"] - today).dt.days
quote_df["quote_status"] = quote_df["valid_till"].apply(
    lambda x: "Expired" if pd.notnull(x) and x < today else "Active"
)

# =================== APPLY FILTERS SAFELY ===================
filtered_df = quote_df.copy()

# Customers
if selected_customers:
    filtered_df = filtered_df[filtered_df["customer_name"].isin(selected_customers)]

# Start date
if start_date:
    filtered_df = filtered_df[filtered_df["transaction_date"] >= pd.to_datetime(start_date)]

# End date
if end_date:
    filtered_df = filtered_df[filtered_df["transaction_date"] <= pd.to_datetime(end_date)]

# Minimum value
if min_value:
    try:
        min_value = float(min_value)
        filtered_df = filtered_df[filtered_df["net_total"] >= min_value]
    except:
        st.warning("⚠️ Min value filter invalid, skipping.")

# Search by quotation ID
if so_search:
    filtered_df = filtered_df[
        filtered_df["quotation_id"].astype(str).str.contains(str(so_search), case=False)
    ]

st.write("### Filtered Data Preview")
st.dataframe(filtered_df.head(10))
st.write("Rows after filters:", len(filtered_df))

# =================== TABS ===================
tab1, tab2, tab3 = st.tabs([
    "Overview",
    "Customer & Item Insights",
    "Trends & Forecast"
])

# =================== TAB 1: OVERVIEW ===================
with tab1:
    st.header("📊 Overview KPIs")

    if not filtered_df.empty:
        total_value = filtered_df["net_total"].sum()
        total_quotes = len(filtered_df)
        active_quotes = (filtered_df["quote_status"] == "Active").sum()
        expired_quotes = (filtered_df["quote_status"] == "Expired").sum()
        avg_value = filtered_df["net_total"].mean()

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Value", f"₹{total_value:,.0f}")
        k2.metric("Quotations", total_quotes)
        k3.metric("Active", active_quotes)
        k4.metric("Expired", expired_quotes)
        k5.metric("Avg Value", f"₹{avg_value:,.0f}")

        status_df = filtered_df["quote_status"].value_counts().reset_index()
        status_df.columns = ["status", "count"]
        st.plotly_chart(px.pie(status_df, names="status", values="count", title="Quotation Status"),
                        use_container_width=True)

        expiring_df = filtered_df[(filtered_df["days_to_expiry"] <= 7) & (filtered_df["days_to_expiry"] >= 0)]
        if len(expiring_df) > 0:
            st.warning(f"⚠️ {len(expiring_df)} quotations expiring within 7 days")

        st.dataframe(filtered_df.sort_values("transaction_date", ascending=False), use_container_width=True)
    else:
        st.info("No quotations available for the selected filters.")

# =================== TAB 2: CUSTOMER & ITEM INSIGHTS ===================
with tab2:
    st.header("👤 Customer Insights")
    if not filtered_df.empty:
        customer_df = filtered_df.groupby("customer_name")["net_total"].sum().reset_index()
        st.plotly_chart(
            px.bar(customer_df.sort_values("net_total", ascending=False).head(10),
                   x="customer_name", y="net_total", title="Top Customers by Quotation Value"),
            use_container_width=True
        )
        st.dataframe(customer_df.sort_values("net_total", ascending=False), use_container_width=True)
    else:
        st.info("No customer data for the selected filters.")

    st.header("📦 Item Insights")
    if not df.empty:
        item_df = df.groupby("item_name")["net_total"].sum().reset_index()
        st.plotly_chart(
            px.bar(item_df.sort_values("net_total", ascending=False).head(10),
                   x="item_name", y="net_total", title="Top Items by Quotation Value"),
            use_container_width=True
        )
        st.dataframe(item_df.sort_values("net_total", ascending=False), use_container_width=True)
    else:
        st.info("No item data available.")

# =================== TAB 3: TRENDS & FORECAST ===================
with tab3:
    st.header("📈 Trends")
    if not filtered_df.empty:
        trend_df = filtered_df.groupby(filtered_df["transaction_date"].dt.date)["net_total"].sum().reset_index()
        st.plotly_chart(
            px.line(trend_df, x="transaction_date", y="net_total", markers=True),
            use_container_width=True
        )

        st.subheader("🔮 90-Day Forecast")
        forecast_df = trend_df.copy()
        forecast_df["transaction_date"] = pd.to_datetime(forecast_df["transaction_date"])
        forecast_df = forecast_df.set_index("transaction_date").asfreq("D", fill_value=0)

        if forecast_df["net_total"].sum() > 0:
            model = ExponentialSmoothing(forecast_df["net_total"], trend="add", seasonal=None)
            model_fit = model.fit()
            forecast = model_fit.forecast(90)

            forecast_plot_df = forecast.reset_index()
            forecast_plot_df.columns = ["date", "forecast_value"]

            st.plotly_chart(
                px.line(forecast_plot_df, x="date", y="forecast_value", markers=True),
                use_container_width=True
            )

            st.download_button(
                "📥 Download Forecast CSV",
                forecast_plot_df.to_csv(index=False).encode("utf-8"),
                "sales_quotation_forecast.csv",
                "text/csv"
            )
        else:
            st.warning("⚠️ Not enough data for forecasting.")
    else:
        st.info("No trend data available for the selected filters.")

# =================== CLOSE CONNECTION ===================
con.close()