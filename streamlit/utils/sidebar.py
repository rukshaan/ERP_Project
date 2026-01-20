# utils/sidebar.py
import streamlit as st
import duckdb
from datetime import datetime, timedelta
from utils.auth import logout   # ✅ unified logout

DB_PATH = "./data/Silver/dev.duckdb"


def render_sidebar():

    # 🔒 AUTH GUARD (DO NOT REMOVE)
    if not st.session_state.get("authenticated", False):
        return None

    st.sidebar.title("📊 ERP Dashboard")

    # ================= DB CONNECTION (SAFE) =================
    con = duckdb.connect(DB_PATH, read_only=True)

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

    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.session_state.start_date = st.date_input(
            "From Date",
            value=st.session_state.start_date,
            min_value=min_date,
            max_value=max_date
        )

    with col2:
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

    customers_df = con.execute("""
        SELECT DISTINCT customer_name
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
        ORDER BY customer_name
    """).df()

    st.session_state.selected_customers = st.sidebar.multiselect(
        "Select Customers",
        options=customers_df["customer_name"].tolist(),
        default=st.session_state.selected_customers
    )

    # ==================================================
    # 📦 ITEMS
    # ==================================================
    st.sidebar.subheader("📦 Items")

    items_df = con.execute("""
        SELECT DISTINCT item_name
        FROM main_prod.fact_final_joined_files
        WHERE open_qty > 0
        ORDER BY item_name
    """).df()

    st.session_state.selected_items = st.sidebar.multiselect(
        "Select Items",
        options=items_df["item_name"].tolist(),
        default=st.session_state.selected_items
    )

    # ==================================================
    # 💰 VALUE FILTER
    # ==================================================
    st.sidebar.subheader("💰 Value Threshold")

    st.session_state.min_value = st.sidebar.number_input(
        "Minimum Order Value (₹)",
        min_value=0,
        value=st.session_state.min_value,
        step=1000
    )

    # ==================================================
    # 📄 ORDER SEARCH
    # ==================================================
    st.sidebar.subheader("📄 Order Search")

    st.session_state.so_search = st.sidebar.text_input(
        "Search by Sales Order ID",
        value=st.session_state.so_search
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
    user_email = st.session_state.get("user", "Unknown User")

    st.sidebar.markdown(
        f"""
        <div style="
            padding: 8px 10px;
            background-color: #f0f2f6;
            border-radius: 8px;
            font-size: 13px;
            margin-bottom: 6px;
        ">
            👤 <strong>{user_email}</strong>
        </div>
        """,
        unsafe_allow_html=True
    )

    # ==================================================
    # 🚪 LOGOUT
    # =================================================
        
    if st.sidebar.button("🚪 Logout"):
        con.close()
        logout()
        # 🔒 CLOSE DB (IMPORTANT)
        con.close()
        return logout()   # 🛑 STOP sidebar execution


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

