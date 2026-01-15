import streamlit as st
from utils.erp import Connect
from streamlit_cookies_manager import EncryptedCookieManager


def init_session():
    """Initialize session state safely"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "erp_conn" not in st.session_state:
        st.session_state.erp_conn = None


def render_login():
    init_session()
    
    cookies = EncryptedCookieManager(
        prefix="erp_dashboard",
        password="super-secret-key"  # 🔐 move to env later
    )

    
    # 🔐 Login UI
    st.title("🔐 ERP Login")
    st.markdown("Please sign in to access the dashboard")

    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("ERP Username / Email")
        password = st.text_input("ERP Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        if not username or not password:
            st.warning("⚠️ Please enter username and password")
            return

        conn = Connect(username=username, password=password)

        if conn.auth_session:
            # ✅ Session
            st.session_state.authenticated = True
            st.session_state.erp_conn = conn
            st.session_state.user = username

            # 🍪 Cookies
            cookies["authenticated"] = "true"
            cookies["user"] = username
            cookies.save()

            st.success("✅ Login successful")
            st.rerun()
        else:
            st.error("❌ Invalid ERP credentials")
    # ⏳ Wait until cookies are ready
    if not cookies.ready():
        st.info("🔄 Initializing authentication...")
        st.stop()

    # 🔁 Auto-login from cookie (safe)
    if (
        cookies.get("authenticated") == "true"
        and not st.session_state.authenticated
    ):
        st.session_state.authenticated = True
        st.session_state.user = cookies.get("user")
        st.rerun()



def logout():
    """Optional logout helper (use later)"""
    cookies = EncryptedCookieManager(
        prefix="erp_dashboard",
        password="super-secret-key"
    )

    if cookies.ready():
        cookies["authenticated"] = "false"
        cookies["user"] = ""
        cookies.save()

    for key in ["authenticated", "user", "erp_conn"]:
        if key in st.session_state:
            del st.session_state[key]

    st.rerun()
