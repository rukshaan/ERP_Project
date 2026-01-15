import streamlit as st
import duckdb
from datetime import datetime, timedelta
from streamlit_cookies_manager import EncryptedCookieManager

# =================== COOKIE MANAGER ===================
cookies = EncryptedCookieManager(
    prefix="my_app",
    password="secret_key_2026!"
)

if not cookies.ready():
    st.stop()

# =================== DATABASE ===================
DB_PATH = "./data/Silver/dev.duckdb"
con = duckdb.connect(DB_PATH, read_only=False)


def render_sidebar():

    # ==================================================
    # 📅 DATE RANGE
    # ==================================================
    st.sidebar.subheader("📅 Date Range")

    min_max_query = """
        SELECT MIN(order_date) AS min_date,
               MAX(delivery_date) AS max_date
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
    """
    date_range = con.execute(min_max_query).df()

    min_date_db = date_range["min_date"][0] if not date_range.empty else None
    max_date_db = date_range["max_date"][0] if not date_range.empty else None

    min_date = min_date_db.date() if min_date_db else (datetime.now() - timedelta(days=365)).date()
    max_date = max_date_db.date() if max_date_db else datetime.now().date()

    # ==================================================
    # 🧠 SESSION INIT (ONCE)
    # ==================================================
    if "initialized" not in st.session_state:
        st.session_state.start_date = min_date
        st.session_state.end_date = max_date
        st.session_state.selected_customers = []
        st.session_state.selected_items = []
        st.session_state.delivery_status = "All"
        st.session_state.min_value = 0
        st.session_state.so_search = ""
        st.session_state.initialized = True

    date_cols = st.sidebar.columns(2)
    with date_cols[0]:
        st.session_state.start_date = st.date_input(
            "From Date",
            value=st.session_state.start_date,
            min_value=min_date,
            max_value=max_date
        )

    with date_cols[1]:
        st.session_state.end_date = st.date_input(
            "To Date",
            value=st.session_state.end_date,
            min_value=min_date,
            max_value=max_date
        )

    # ==================================================
    # 👥 CUSTOMERS
    # ==================================================
    st.sidebar.subheader("👥 Customers")

    customer_query = """
        SELECT DISTINCT customer_name
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
        ORDER BY customer_name
    """
    customers_df = con.execute(customer_query).df()
    all_customers = customers_df["customer_name"].tolist() if not customers_df.empty else []

    st.session_state.selected_customers = st.sidebar.multiselect(
        "Select Customers",
        options=all_customers,
        default=st.session_state.selected_customers,
        help="Leave empty to show all customers"
    )

    # ==================================================
    # 📦 ITEMS
    # ==================================================
    st.sidebar.subheader("📦 Items")

    item_query = """
        SELECT DISTINCT item_name
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
        ORDER BY item_name
    """
    items_df = con.execute(item_query).df()
    all_items = items_df["item_name"].tolist() if not items_df.empty else []

    st.session_state.selected_items = st.sidebar.multiselect(
        "Select Items",
        options=all_items,
        default=st.session_state.selected_items,
        help="Leave empty to show all items"
    )

    # ==================================================
    # 💰 MIN VALUE
    # ==================================================
    st.sidebar.subheader("💰 Value Threshold")

    st.session_state.min_value = st.sidebar.number_input(
        "Minimum Order Value (₹)",
        min_value=0,
        value=st.session_state.min_value,
        step=1000
    )

    # ==================================================
    # 📄 SALES ORDER SEARCH
    # ==================================================
    st.sidebar.subheader("📄 Order Search")

    st.session_state.so_search = st.sidebar.text_input(
        "Search by Sales Order ID",
        value=st.session_state.so_search,
        placeholder="Enter order ID..."
    )

    # ==================================================
    # 🔄 CLEAR FILTERS
    # ==================================================
    if st.sidebar.button("🔄 Clear All Filters"):
        st.session_state.start_date = min_date
        st.session_state.end_date = max_date
        st.session_state.selected_customers = []
        st.session_state.selected_items = []
        st.session_state.delivery_status = "All"
        st.session_state.min_value = 0
        st.session_state.so_search = ""

    st.sidebar.divider()

    # ==================================================
    # 🚪 LOGOUT (SAFE)
    # ==================================================
    if st.sidebar.button("🚪 Logout"):
        if "erp_conn" in st.session_state:
            st.session_state.erp_conn.logout()

        cookies["authenticated"] = ""
        cookies["user"] = ""
        cookies.save()

        st.session_state.clear()
        st.switch_page("pages/0_Login.py")

    # ==================================================
    # 🔁 RETURN FILTERS
    # ==================================================
    return {
        "start_date": st.session_state.start_date,
        "end_date": st.session_state.end_date,
        "customers": st.session_state.selected_customers,
        "items": st.session_state.selected_items,
        "delivery_status": st.session_state.delivery_status,
        "min_value": st.session_state.min_value,
        "so_search": st.session_state.so_search,
    }
