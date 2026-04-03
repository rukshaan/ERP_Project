# utils/auth.py
import streamlit as st
import hashlib
from streamlit_cookies_manager import EncryptedCookieManager
from utils.db import get_pg_connection
from utils.init_db import init_auth_tables

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
    # Ensure database is initialized
    if "db_initialized" not in st.session_state:
        init_auth_tables()
        st.session_state.db_initialized = True
        
    st.session_state.setdefault("authenticated", False)
    st.session_state.setdefault("user", None)
    st.session_state.setdefault("role", None)  # ✅ Added role


# ================= AUTH HELPERS =================
def hash_password(password):
    return hashlib.sha256(password.strip().encode()).hexdigest()

def verify_credentials(username, password):
    username = username.strip()
    password = password.strip()
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT u.password_hash, r.name 
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.username = %s
        """, (username,))
        result = cur.fetchone()
        if result:
            db_hash, role = result
            if db_hash == hash_password(password):
                return True, role
        return False, None
    except Exception as e:
        # Check if it's a connection error or a query error
        if "Failed to connect to PostgreSQL" in str(e):
            st.error(f"🔌 Database Connection Error: {str(e)}")
        else:
            st.error(f"❌ Database Query Error: {str(e)}")
        return False, None
    finally:
        if 'cur' in locals() and cur: cur.close()
        if 'conn' in locals() and conn: conn.close()

# ================= LOGIN PAGE =================
def render_login():
    init_auth_state()
    cookies = get_cookies()

    st.title("🔐 ERP Login")
    st.markdown("Please sign in to access the dashboard")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        if not username or not password:
            st.warning("⚠️ Please enter username and password")
            return

        with st.spinner("Authenticating..."):
            is_valid, role = verify_credentials(username, password)

        if is_valid:
            st.session_state.authenticated = True
            st.session_state.user = username
            st.session_state.role = role

            cookies["authenticated"] = "true"
            cookies["user"] = username
            cookies["role"] = role
            cookies.save()

            st.success("✅ Login successful")
            st.rerun()
        else:
            st.error("❌ Invalid credentials")

    # 🔁 AUTO RESTORE LOGIN (ON REFRESH)
    if (
        cookies.get("authenticated") == "true"
        and not st.session_state.authenticated
    ):
        user = cookies.get("user")
        role = cookies.get("role")

        if user and role:
            st.session_state.authenticated = True
            st.session_state.user = user
            st.session_state.role = role
            st.rerun()


# ================= LOGOUT =================
def logout():
    cookies = get_cookies()

    # Invalidate cookies
    cookies["authenticated"] = ""
    cookies["user"] = ""
    cookies["role"] = ""
    cookies.save()

    # Clear Streamlit session
    for key in list(st.session_state.keys()):
        del st.session_state[key]


# ================= AUTH GUARD =================
def require_auth(roles=None):
    init_auth_state()
    cookies = get_cookies()

    # ❌ No cookie → no restore
    if cookies.get("authenticated") != "true":
        st.session_state.authenticated = False
        render_login()
        st.stop()

    # ✅ Restore session from cookie
    if not st.session_state.authenticated:
        user = cookies.get("user")
        role = cookies.get("role")

        if user and role:
            st.session_state.authenticated = True
            st.session_state.user = user
            st.session_state.role = role

    # ❌ Still not authenticated
    if not st.session_state.authenticated:
        render_login()
        st.stop()

    # 👮 Role check
    if roles:
        if isinstance(roles, str):
            roles = [roles]
        if st.session_state.role not in roles:
            st.error("🚫 Access Denied: Insufficient Permissions")
            st.stop()
