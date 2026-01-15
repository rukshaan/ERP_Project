# login.py
import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager
from utils.erp import Connect

COOKIE_PREFIX = "erp_dashboard"
COOKIE_PASSWORD = "super-secret-key"  # move to env later


def get_cookies():
    """Create cookie manager ONCE"""
    if "cookies" not in st.session_state:
        st.session_state.cookies = EncryptedCookieManager(
            prefix=COOKIE_PREFIX,
            password=COOKIE_PASSWORD
        )

        if not st.session_state.cookies.ready():
            st.stop()

    return st.session_state.cookies


def init_session():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "erp_conn" not in st.session_state:
        st.session_state.erp_conn = None


def render_login():
    init_session()
    cookies = get_cookies()

    st.title("🔐 ERP Login")
    st.markdown("Please sign in to access the dashboard")

    with st.form("login_form"):
        username = st.text_input("ERP Username / Email")
        password = st.text_input("ERP Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        if not username or not password:
            st.warning("⚠️ Please enter username and password")
            return

        conn = Connect(username=username, password=password)

        if conn.auth_session:
            st.session_state.authenticated = True
            st.session_state.erp_conn = conn
            st.session_state.user = username

            cookies["authenticated"] = "true"
            cookies["user"] = username
            cookies.save()

            st.success("✅ Login successful")
            st.rerun()
        else:
            st.error("❌ Invalid ERP credentials")

    # 🔁 Auto-login
    if cookies.get("authenticated") == "true" and not st.session_state.authenticated:
        st.session_state.authenticated = True
        st.session_state.user = cookies.get("user")
        st.rerun()


def logout():
    cookies = get_cookies()   # ✅ reuse, do NOT create new

    # clear cookies
    cookies.clear()

    # clear session
    for key in list(st.session_state.keys()):
        del st.session_state[key]

    st.rerun()
