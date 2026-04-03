import streamlit as st
from utils.auth import require_auth, hash_password
from utils.db import get_pg_connection
from utils.sidebar import render_sidebar
import pandas as pd

# =================== PAGE CONFIG ===================
st.set_page_config(page_title="Admin Management", layout="wide")

# =================== AUTH GUARD ===================
# Only admins can access this page
require_auth(roles='admin')   

# =================== SIDEBAR ===================
render_sidebar()

st.title("👥 Admin Management")
st.markdown("Manage users and roles for the ERP Dashboard")

# =================== USER CREATION ===================
st.subheader("➕ Create New User")

with st.form("create_user_form", clear_on_submit=True):
    new_username = st.text_input("Username")
    new_password = st.text_input("Password", type="password")
    
    # Get roles from DB
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM roles")
    roles = cur.fetchall()
    role_options = {name: id for id, name in roles}
    cur.close()
    conn.close()
    
    selected_role_name = st.selectbox("Assign Role", options=list(role_options.keys()))
    submit_create = st.form_submit_button("Create User")

if submit_create:
    if not new_username or not new_password:
        st.warning("⚠️ Username and Password are required")
    else:
        conn = get_pg_connection()
        cur = conn.cursor()
        try:
            # Check if user exists
            cur.execute("SELECT id FROM users WHERE username = %s", (new_username,))
            if cur.fetchone():
                st.error(f"❌ User '{new_username}' already exists")
            else:
                hashed_pw = hash_password(new_password)
                role_id = role_options[selected_role_name]
                cur.execute(
                    "INSERT INTO users (username, password_hash, role_id) VALUES (%s, %s, %s)",
                    (new_username, hashed_pw, role_id)
                )
                conn.commit()
                st.success(f"✅ User '{new_username}' created successfully as '{selected_role_name}'")
        except Exception as e:
            st.error(f"❌ Error creating user: {e}")
        finally:
            cur.close()
            conn.close()

st.divider()

# =================== USER LISTING ===================
st.subheader("📋 Existing Users")

conn = get_pg_connection()
cur = conn.cursor()
cur.execute("""
    SELECT u.username, r.name as role, u.created_at 
    FROM users u
    JOIN roles r ON u.role_id = r.id
    ORDER BY u.created_at DESC
""")
users_data = cur.fetchall()
cur.close()
conn.close()

if users_data:
    df_users = pd.DataFrame(users_data, columns=["Username", "Role", "Created At"])
    st.dataframe(df_users, use_container_width=True)
else:
    st.info("No users found in the database.")

st.divider()

# =================== ROLE MANAGEMENT ===================
st.header("🔑 Role Management")

col_r1, col_r2 = st.columns([1, 1])

with col_r1:
    st.subheader("➕ Create New Role")
    with st.form("create_role_form", clear_on_submit=True):
        new_role_name = st.text_input("Role Name")
        submit_role = st.form_submit_button("Create Role")

    if submit_role:
        if not new_role_name:
            st.warning("⚠️ Role name is required")
        else:
            conn = get_pg_connection()
            cur = conn.cursor()
            try:
                # Check if role exists
                cur.execute("SELECT id FROM roles WHERE name = %s", (new_role_name,))
                if cur.fetchone():
                    st.error(f"❌ Role '{new_role_name}' already exists")
                else:
                    cur.execute("INSERT INTO roles (name) VALUES (%s)", (new_role_name,))
                    conn.commit()
                    st.success(f"✅ Role '{new_role_name}' created successfully")
                    st.rerun() # Refresh to update user creation dropdown
            except Exception as e:
                st.error(f"❌ Error creating role: {e}")
            finally:
                cur.close()
                conn.close()

with col_r2:
    st.subheader("📋 Existing Roles")
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT name FROM roles ORDER BY name ASC")
    roles_data = cur.fetchall()
    cur.close()
    conn.close()
    
    if roles_data:
        df_roles = pd.DataFrame(roles_data, columns=["Role Name"])
        st.dataframe(df_roles, use_container_width=True)
    else:
        st.info("No roles found.")
