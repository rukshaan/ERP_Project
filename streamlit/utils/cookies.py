import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager

def get_cookies():
    if "cookies_manager" not in st.session_state:
        st.session_state.cookies_manager = EncryptedCookieManager(
            prefix="app",
            password="secret_key_2026!"
        )
    return st.session_state.cookies_manager
