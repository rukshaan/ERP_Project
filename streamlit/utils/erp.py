import os
import requests


import requests


class Connect:
    def __init__(self, username=None, password=None, session_id=None):
        self.host = "https://demoerp.enkinext.com/api"
        self.session = requests.Session()
        self.auth_session = None  # this will store SID (string)

        # 🔁 Restore session from cookie
        if session_id:
            self.session.cookies.set("sid", session_id)
            self.auth_session = session_id
            return

        # 🔐 Fresh login
        if username and password:
            self.login(username, password)

    def login(self, username, password):
        response = self.session.post(
            f"{self.host}/method/login",
            data={"usr": username, "pwd": password},
        )

        if response.status_code == 200 and response.json().get("message") == "Logged In":
            # ✅ Extract ERP session id (SID)
            sid = self.session.cookies.get("sid")

            if sid:
                self.auth_session = sid
                return True

        return False

    def logout(self):
        if self.auth_session:
            self.session.get(f"{self.host}/method/logout")
            self.auth_session = None
