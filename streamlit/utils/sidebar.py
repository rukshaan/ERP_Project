import streamlit as st
import duckdb
from datetime import datetime, timedelta


def render_sidebar():
    # 🔒 Hide sidebar if not authenticated
    if not st.session_state.get("authenticated"):
        return None

    # =================== COOKIE MANAGER ===================
    from streamlit_cookies_manager import EncryptedCookieManager
    cookies = EncryptedCookieManager(
        prefix="my_app",
        password="secret_key_2026!"
    )

    if not cookies.ready():
        st.stop()

    # =================== DATABASE ===================
    DB_PATH = "./data/Silver/dev.duckdb"
    con = duckdb.connect(DB_PATH, read_only=False)

    st.sidebar.title("📊 ERP Dashboard")

    # ==================================================
    # 📅 DATE RANGE
    # ==================================================
    st.sidebar.subheader("📅 Date Range")

    date_df = con.execute("""
        SELECT MIN(order_date) AS min_date,
               MAX(delivery_date) AS max_date
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
    """).df()

    min_date = date_df["min_date"][0].date()
    max_date = date_df["max_date"][0].date()

    # Initialize session values once
    if "initialized" not in st.session_state:
        st.session_state.start_date = min_date
        st.session_state.end_date = max_date
        st.session_state.selected_customers = []
        st.session_state.selected_items = []
        st.session_state.delivery_status = "All"
        st.session_state.min_value = 0
        st.session_state.so_search = ""
        st.session_state.initialized = True

    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.session_state.start_date = st.date_input(
            "From",
            st.session_state.start_date,
            min_value=min_date,
            max_value=max_date
        )
    with col2:
        st.session_state.end_date = st.date_input(
            "To",
            st.session_state.end_date,
            min_value=min_date,
            max_value=max_date
        )

    # ==================================================
    # 👥 CUSTOMERS
    # ==================================================
    st.sidebar.subheader("👥 Customers")

    customers = con.execute("""
        SELECT DISTINCT customer_name
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
        ORDER BY customer_name
    """).df()["customer_name"].tolist()

    st.session_state.selected_customers = st.sidebar.multiselect(
        "Select Customers",
        customers,
        st.session_state.selected_customers
    )

    # ==================================================
    # 📦 ITEMS
    # ==================================================
    st.sidebar.subheader("📦 Items")

    items = con.execute("""
        SELECT DISTINCT item_name
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
        ORDER BY item_name
    """).df()["item_name"].tolist()

    st.session_state.selected_items = st.sidebar.multiselect(
        "Select Items",
        items,
        st.session_state.selected_items
    )

    # ==================================================
    # 🚚 DELIVERY STATUS
    # ==================================================
    st.sidebar.subheader("🚚 Delivery Status")

    delivery_options = [
        "All",
        "On Time",
        "Overdue",
        "Upcoming",
        "Urgent (< 3 days)"
    ]

    st.session_state.delivery_status = st.sidebar.selectbox(
        "Delivery Status",
        delivery_options,
        delivery_options.index(st.session_state.delivery_status)
    )

    # ==================================================
    # 💰 MIN VALUE
    # ==================================================
    st.sidebar.subheader("💰 Minimum Order Value")

    st.session_state.min_value = st.sidebar.number_input(
        "Min Value (₹)",
        min_value=0,
        step=1000,
        value=st.session_state.min_value
    )

    # ==================================================
    # 📄 SALES ORDER SEARCH
    # ==================================================
    st.sidebar.subheader("📄 Sales Order Search")

    st.session_state.so_search = st.sidebar.text_input(
        "Sales Order ID",
        st.session_state.so_search
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
    # 🚪 LOGOUT
    # ==================================================
    from login import logout

    if st.sidebar.button("🚪 Logout"):
        logout()


    # ==================================================
    # 🔁 RETURN FILTERS (CONTRACT)
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
