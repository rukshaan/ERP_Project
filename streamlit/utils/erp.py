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
        print(f"\n🔐 Attempting login for user: {username}")
        print(f"API Endpoint: {self.host}/method/login")
        
        response = self.session.post(
            f"{self.host}/method/login",
            data={"usr": username, "pwd": password},
        )
        
        # 🔍 DEBUG - Full response details
        print(f"\n📊 Response Status Code: {response.status_code}")
        print(f"📄 Response Body: {response.text}")
        
        try:
            response_json = response.json()
            print(f"📦 Response JSON: {response_json}")
        except Exception as e:
            print(f"⚠️ Could not parse JSON: {e}")
            response_json = None
        
        print(f"🍪 Cookies set: {self.session.cookies.get_dict()}")
        
        # Check login success
        if response.status_code == 200 and response_json and response_json.get("message") == "Logged In":
            # ✅ Extract ERP session id (SID)
            sid = self.session.cookies.get("sid")
            print(f"✅ SID extracted: {sid}")
            
            if sid:
                self.auth_session = sid
                print(f"✅ Login successful! Session ID: {sid}")
                return True
            else:
                print("❌ Login succeeded but no SID cookie found!")
        else:
            print("❌ Login failed - status code or message validation failed")
            if response_json:
                print(f"   Expected message: 'Logged In'")
                print(f"   Actual message: {response_json.get('message', 'N/A')}")

        return False

    def logout(self):
        if self.auth_session:
            self.session.get(f"{self.host}/method/logout")
            self.auth_session = None
