# utils/auth.py
import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager
from utils.erp import Connect

# ================= CONFIG =================
COOKIE_PREFIX = "erp_dashboard"
COOKIE_PASSWORD = "super-secret-key"   # move to env later

# ================= COOKIE MANAGER =================
def get_cookies():
    if "_cookies" not in st.session_state:
        cookies = EncryptedCookieManager(
            prefix=COOKIE_PREFIX,
            password=COOKIE_PASSWORD
        )
        if not cookies.ready():
            st.stop()
        st.session_state._cookies = cookies
    return st.session_state._cookies


# ================= SESSION INIT =================
def init_auth_state():
    st.session_state.setdefault("authenticated", False)
    st.session_state.setdefault("user", None)
    st.session_state.setdefault("erp_conn", None)


# ================= LOGIN PAGE =================
def render_login():
    init_auth_state()
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

        with st.spinner("Authenticating..."):
            conn = Connect(username=username, password=password)

        if conn.auth_session:
            st.session_state.authenticated = True
            st.session_state.user = username
            st.session_state.erp_conn = conn

            cookies["authenticated"] = "true"
            cookies["user"] = username
            cookies["sid"] = conn.auth_session
            cookies.save()

            st.success("✅ Login successful")
            st.rerun()
        else:
            st.error("❌ Invalid ERP credentials")

    # 🔁 AUTO RESTORE LOGIN (ON REFRESH)
    if (
        cookies.get("authenticated") == "true"
        and not st.session_state.authenticated
    ):
        sid = cookies.get("sid")
        user = cookies.get("user")

        if sid and user:
            st.session_state.authenticated = True
            st.session_state.user = user
            st.session_state.erp_conn = Connect(session_id=sid)
            st.rerun()


# ================= LOGOUT =================
# utils/auth.py

# utils/auth.py

# utils/auth.py

def logout():
    cookies = get_cookies()

    # 1️⃣ Logout from ERP session (safe)
    try:
        if st.session_state.get("erp_conn"):
            st.session_state.erp_conn.logout()
    except Exception:
        pass

    # 2️⃣ Invalidate cookies (VERY IMPORTANT)
    cookies["authenticated"] = ""
    cookies["user"] = ""
    cookies["sid"] = ""
    cookies.save()

    # 3️⃣ Clear Streamlit session
    for key in list(st.session_state.keys()):
        del st.session_state[key]





# ================= AUTH GUARD =================
# utils/auth.py

def require_auth():
    init_auth_state()
    cookies = get_cookies()

    # ❌ No cookie → no restore
    if cookies.get("authenticated") != "true":
        st.session_state.authenticated = False
        render_login()
        st.stop()

    # ✅ Restore session from cookie
    if not st.session_state.authenticated:
        sid = cookies.get("sid")
        user = cookies.get("user")

        if sid and user:
            st.session_state.authenticated = True
            st.session_state.user = user
            st.session_state.erp_conn = Connect(session_id=sid)
            return

    # ❌ Still not authenticated
    if not st.session_state.authenticated:
        render_login()
        st.stop()
