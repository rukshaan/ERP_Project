# login.py
from utils.auth import render_login, logout, require_auth

# Re-export functions so old imports still work
__all__ = [
    "render_login",
    "logout",
    "require_auth",
]
