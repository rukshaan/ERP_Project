import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime
from utils.auth import require_auth
from utils.sidebar import render_sidebar
import sys
import os

# Add utils folder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "../utils"))

from prediction_utils import (
    predict_product_sales,
    predict_customers,
    customer_product_prediction
)

st.set_page_config(layout="wide")

# ---------------------- AUTH ----------------------
require_auth()

# ---------------------- DB ----------------------
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

# ---------------------- TITLE ----------------------
st.title("📊 Sales & Customer Forecasts")

# ---------------------- FILTERS ----------------------
filters = render_sidebar()
if filters is None:
    st.stop()

start_date = filters["start_date"]
end_date = filters["end_date"]
selected_customers = filters["customers"]
selected_items = filters["items"]

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
FROM main_prod.fact_final_joined_files f
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

# CSV download
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

# CSV download
csv_forecast = future_forecast.to_csv(index=False).encode("utf-8")
st.download_button(
    "📥 Download Forecast CSV",
    csv_forecast,
    f"product_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    "text/csv"
)

# ---------------------- TOP ITEMS CHART ----------------------
st.subheader("Top 15 Items by Forecasted Units")
top_items_forecast = future_forecast.groupby('item_name')['predicted_units'].sum().sort_values(ascending=False).head(15)
st.bar_chart(top_items_forecast)

# ---------------------- CUSTOMER FORECAST ----------------------
st.header("👤 Likely Active Customers")

customer_forecast_df = predict_customers(df)
st.dataframe(customer_forecast_df, use_container_width=True)

# CSV download
csv_cust = customer_forecast_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "📥 Download Customer Forecast CSV",
    csv_cust,
    f"customer_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    "text/csv"
)

# ---------------------- CUSTOMER → ITEM → DATE FORECAST ----------------------
st.header("📋 Predicted Items per Customer")

customer_product_df = customer_product_prediction(df, forecast_days=forecast_days)

# Convert predicted_date to just date (remove timestamp)
customer_product_df['predicted_date'] = pd.to_datetime(
    customer_product_df['predicted_date']
).dt.date

# Sort for easy lookup
customer_product_df = customer_product_df.sort_values(
    ["predicted_date","customer_name", "predicted_units"],
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

# CSV download
csv_cust_prod = customer_product_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "📥 Download Customer-Item Forecast CSV",
    csv_cust_prod,
    f"customer_item_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    "text/csv"
)

# ---------------------- FOOTER ----------------------
con.close()
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
