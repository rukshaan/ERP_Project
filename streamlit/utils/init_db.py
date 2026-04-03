# utils/init_db.py
import psycopg2
import hashlib
from utils.db import get_pg_connection

def hash_password(password):
    return hashlib.sha256(password.strip().encode()).hexdigest()

def init_auth_tables():
    """Initializes the roles and users tables in the PostgreSQL database."""
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        
        print("Creating 'roles' table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS roles (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            );
        """)
        
        print("Creating 'users' table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role_id INTEGER REFERENCES roles(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Seed default roles
        print("Seeding default roles...")
        roles = [('admin',), ('viewer',)]
        cur.executemany("""
            INSERT INTO roles (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING;
        """, roles)
        
        # Check if users exist, seed default admin if none
        cur.execute("SELECT COUNT(*) FROM users")
        if cur.fetchone()[0] == 0:
            print("No users found. Creating default 'admin' user...")
            # Need role_id for 'admin'
            cur.execute("SELECT id FROM roles WHERE name = 'admin'")
            admin_role_id = cur.fetchone()[0]
            
            admin_user = 'admin'
            admin_pass = 'admin'
            cur.execute("""
                INSERT INTO users (username, password_hash, role_id)
                VALUES (%s, %s, %s)
            """, (admin_user, hash_password(admin_pass), admin_role_id))
            print(f"✅ Default admin user created: {admin_user} / {admin_pass}")
        
        conn.commit()
        print("✅ Authentication database initialized successfully.")
        return True
        
    except Exception as e:
        print(f"❌ Error initializing auth tables: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    init_auth_tables()
