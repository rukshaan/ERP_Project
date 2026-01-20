import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime

from utils.auth import require_auth        # ✅ unified auth
from utils.sidebar import render_sidebar

# =================== PAGE CONFIG ===================
st.set_page_config(layout="wide")

# =================== AUTH GUARD ===================
require_auth()   # 🔐 restores session from cookies on refresh

# =================== DB CONNECTION ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

# =================== SIDEBAR FILTERS ===================
filters = render_sidebar()
if filters is None:
    st.stop()

st.title("🚚 Delivery Performance & Insights")

# =================== WHERE CLAUSE BUILDER ===================
def build_filter_clause(filters: dict):
    clauses = ["f.open_qty > 0"]

    clauses.append(
        f"f.order_date BETWEEN '{filters['start_date']}' AND '{filters['end_date']}'"
    )

    if filters["customers"]:
        customers = "', '".join(filters["customers"])
        clauses.append(f"f.customer_name IN ('{customers}')")

    if filters["items"]:
        items = "', '".join(filters["items"])
        clauses.append(f"""
            f.item_code IN (
                SELECT item_code
                FROM main_prod.dim_item
                WHERE item_name IN ('{items}')
            )
        """)

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

c1, c2, c3 = st.columns(3)
c1.metric("On-Time Orders", int(kpi_df.iloc[0]["on_time"]))
c2.metric("Late Orders", int(kpi_df.iloc[0]["late"]))
c3.metric("Average Delay (days)", round(kpi_df.iloc[0]["avg_delay"], 1))

# =================== DELIVERY PIE ===================
chart_df = pd.DataFrame({
    "Status": ["On-Time", "Late"],
    "Orders": [kpi_df.iloc[0]["on_time"], kpi_df.iloc[0]["late"]]
})

st.plotly_chart(
    px.pie(chart_df, names="Status", values="Orders", title="Delivery Performance"),
    use_container_width=True
)

# =================== TOP CUSTOMERS ===================
st.subheader("🏆 Top Customers")

customer_df = con.execute(f"""
SELECT customer_name, SUM(amount) AS total_value
FROM main_prod.fact_final_joined_files f
WHERE {where_clause}
GROUP BY customer_name
ORDER BY total_value DESC
LIMIT 10
""").df()

if customer_df.empty:
    st.info("No customer data available.")
else:
    st.plotly_chart(
        px.bar(customer_df, x="customer_name", y="total_value",
               title="Top Customers by Order Value"),
        use_container_width=True
    )

# =================== CUSTOMER DRILLDOWN ===================
st.subheader("📦 Customer Item Breakdown")

selected_customer = st.selectbox(
    "Select a customer",
    customer_df["customer_name"].unique()
)

item_df = con.execute(f"""
SELECT i.item_name,
       SUM(f.qty) AS total_qty,
       SUM(f.amount) AS total_value
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE {where_clause}
  AND f.customer_name = '{selected_customer}'
GROUP BY i.item_name
ORDER BY total_qty DESC
""").df()

if not item_df.empty:
    fig = px.pie(
        item_df,
        names="item_name",
        values="total_qty",
        hole=0.3,
        title=f"Item Quantity Share – {selected_customer}"
    )
    fig.update_traces(textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No item data available.")

# =================== TOP ITEMS ===================
st.subheader("🔥 Top Items by Quantity")

top_items = con.execute(f"""
SELECT i.item_name, SUM(f.qty) AS total_qty
FROM main_prod.fact_final_joined_files f
JOIN main_prod.dim_item i ON f.item_code = i.item_code
WHERE {where_clause}
GROUP BY i.item_name
ORDER BY total_qty DESC
LIMIT 10
""").df()

if not top_items.empty:
    st.plotly_chart(
        px.bar(top_items, x="item_name", y="total_qty",
               title="Top Items by Quantity"),
        use_container_width=True
    )

# =================== FOOTER ===================
con.close()
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
