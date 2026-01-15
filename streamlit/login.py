import streamlit as st
from utils.erp import Connect
from utils.cookies import get_cookies


# --------------------------------------------------
# Initialize session state safely
# --------------------------------------------------
def init_session():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "erp_conn" not in st.session_state:
        st.session_state.erp_conn = None


# --------------------------------------------------
# Render Login Page
# --------------------------------------------------
def render_login():
    init_session()
    cookies = get_cookies()

    # ⏳ WAIT until cookies are ready
    if not cookies.ready():
        st.info("🔄 Initializing authentication...")
        st.stop()

    # 🔁 AUTO LOGIN
    if not st.session_state.authenticated:
        cookie_user = cookies.get("user", None)
        cookie_sid = cookies.get("session_id", None)

        if cookie_user and cookie_sid:
            conn = Connect(session_id=cookie_sid)
            try:
                res = conn.session.get(
                    f"{conn.host}/method/frappe.auth.get_logged_user"
                )
                if res.status_code == 200:
                    st.session_state.authenticated = True
                    st.session_state.user = cookie_user
                    st.session_state.erp_conn = conn
                    st.rerun()
            except Exception:
                cookies.clear()
                cookies.save()

    if st.session_state.authenticated:
        return

    # 🔐 LOGIN FORM
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
            st.session_state.user = username
            st.session_state.erp_conn = conn

            cookies["user"] = username
            cookies["session_id"] = conn.auth_session
            cookies.save()

            st.success("✅ Login successful")
            st.rerun()
        else:
            st.error("❌ Invalid ERP credentials")

# --------------------------------------------------
# Logout
# --------------------------------------------------
def logout():
    cookies = get_cookies()

    if st.session_state.get("erp_conn"):
        try:
            st.session_state.erp_conn.logout()
        except Exception:
            pass

    if cookies.ready():
        cookies.clear()
        cookies.save()

    for key in list(st.session_state.keys()):
        del st.session_state[key]

    st.rerun()
