import streamlit as st
import duckdb

from utils.auth import require_auth
from utils.sidebar import render_sidebar

from utils.sales_components import (
    render_sales_overview,
    render_backlog,
    render_delivery_insights,
    render_sales_order_forecast
)

from utils.prediction_utils import (
    predict_product_sales,
    predict_customers,
    customer_product_prediction
)
# =================== PAGE CONFIG ===================
st.set_page_config(layout="wide")

# =================== AUTH GUARD ===================
require_auth()

# =================== SIDEBAR FILTERS ===================
filters = render_sidebar()
if filters is None:
    st.stop()

# =================== DB CONNECTION ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=True)

# =================== TABS ===================
st.title("📦 Sales Order Hub")

tab1, tab2, tab3, tab4 = st.tabs(["Sales Order Overview", "Sales Order Backlog", "Sales Order Delivery Insights","Sales Order Forecast"])

with tab1:
    render_sales_overview(con, filters)

with tab2:
    render_backlog(con, filters)

with tab3:
    render_delivery_insights(con, filters)

with tab4:
    render_sales_order_forecast(con, filters)

# =================== FOOTER ===================
con.close()
