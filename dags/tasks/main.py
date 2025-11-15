import requests
import os
 
import pandas as pd
from airflow.models import Variable
 
 
class Connect:
    _self = None
 
    def __init__(self):
        
        self.auth_session = None
 
        self.host = f'https://{Variable.get("ERP_HOST")}/api'
        print("this is for testing.....",self.host)
        self.login()
        
 
    # Login To Erp Next
    def login(self):
        auth_session = requests.Session()
 
        # Authenticating user
        response = auth_session.post(
            f"{self.host}/method/login",
            data={"usr": Variable.get("ERP_USER"), "pwd": Variable.get("ERP_PASS")},
        )
        print("response is ",response)
 
        # Storing Request session for further requests
        self.auth_session = auth_session
 
        return self
 
    def get_logged_user(self):
        if self.auth_session:
            email='rukku@gmail.com'
            response= self.auth_session.get(
                "https://demoerp.enkinext.com/api/resource/User?limit=50&fields=[\"*\"]"
            )
            if response.status_code == 200:
                user_datas = response.json()
                users = user_datas.get("data", [])
                emails=[]
                for user in users:
                    email = user.get("email")
                    emails.append(email)
                return user_datas
            else:
                return "Error fetching user data"
        return False
 
    # Logout/Destroy Current Session if any
    def logout(self):
        if self.auth_session:
            return self.auth_session.get(f"{self.host}/method/logout")
        else:
            return "Not Logged In"
 
 
class API:
    def __init__(self):
        connect = Connect()
        self.connection = connect.auth_session
        # self.host = f'http://{os.environ.get("ERP_HOST")}'
        self.host = connect.host
        
 
    
 
 
    # Get Results of Doctype In JSON format If any Params, it should be in string format according to ERPNext to get
    # all results with fields=["item_name","item_code"] check doctype fields, to get all the results at once = limit=*
    def results(self, doctype, params=None):
        return self.connection.get(
            url=f"{self.host}/resource/{doctype}",
            params=params,
        ).json()
 
    def get_dataframe(self, doctype, fields='["*"]', params=None):
        try:
 
            data = self.results(doctype + f'?fields={fields}&limit_page_length=999', params=params)["data"]
 
            return pd.DataFrame(data)
        except Exception as e:
            print(str(e))
            return None
    
    def get_doc_sales(self, doctype,name, params=None):
        try:
            path = f'{doctype}/{name}'
            data = self.results(path, params=params)["data"]
            #print(data)
            #df = pd.DataFrame(data)
            #nonetype to string
            #df = df.fillna('')
            #df = df.replace('None','')
            return data
        except Exception as e:
            print(str(e))
            return None                   
        
 
    # Get Count Value of Doc Lists
    def get_count_of_doctype(self, doctype):
        return self.connection.get(
            url=f"{self.host}/method/frappe.client.get_count",
            params={"doctype": doctype},
        ).json()
 
    # Get Single Result BY Doctype and DOC-ID/name
    def single_result(self, doctype, docID, params=None):
        return self.connection.get(
            url=f"{self.host}/resource/{doctype}/{docID}",
            params={"doctype": doctype, "name": docID},
            headers={
                "Accept": "application/json",
            },
        ).json()
 
    # get Submitted Linked Docs
    def get_submitted_linked_docs(self, doctype, docId):
        return self.connection.get(
            url=f"{self.host}/method/frappe.desk.form.linked_with.get_submitted_linked_docs",
            params={"doctype": doctype, "name": docId},
            headers={"Accept": "application/json"},
        ).json()
 
    # Get All the Linkable Docs List
    def get_linked_doc_types(self, doctype):
        return self.connection.get(
            url=f"{self.host}/method/frappe.desk.form.linked_with.get_linked_doctypes",
            params={"doctype": doctype},
        )
 
    # This will give all the linked docs with more details
    def get_lk(self, doctype, docname):
        return self.connection.get(
            url=f"{self.host}/method/frappe.desk.form.linked_with.get",
            params={"doctype": doctype, "docname": docname},
        )
 
    # Cancel all submitted Linked Docs
    def cancel_link_doc(self, doctype, docId):
        return self.connection.post(
            url=f"{self.host}/method/frappe.client.cancel",
            params={"doctype": doctype, "name": docId},
            headers={"Accept": "application/json"},
        )
 
    # Delete Doc
    def delete(self, doctype, docId):
        # Deleting Doc
        response = self.connection.delete(
            url=f"{self.host}/resource/{doctype}/{docId}",
            headers={
                "Accept": "application/json",
            },
        )
 
        return response
 
    # Create Resource in the DB [ERPNext]
    # doctype = Document Type
    # data = payloads in dict format
    def create(self, doctype, data):
        response = self.connection.post(
            url=f"{self.host}/resource/{doctype}",
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            data=data,
           
        )
        return response
 
    def update(self, doctype, name, data):
        return self.connection.put(
            url=f"{self.host}/resource/{doctype}/{name}", data=data
        )
 
    # Rename Doc Name
    def rename_doc(self, doctype, old_name, new_name, merge=False):
        return self.connection.put(
            url=f"{self.host}/method/frappe.client.rename_doc",
            params={
                "doctype": doctype,
                "old_name": old_name,
                "new_name": new_name,
                "merge": merge,
            },
        )
    
    def make_fields(self, columns):
        
        fields_colums = ""
        for col in columns:
            fields_colums += f'"{col}",'
 
        fields_colums = f'[ {fields_colums[:-1]} ]'
        return fields_colums
 
    def method(self, url, params):
        return self.connection.post(url=self.host, params=params).json()
    
    def post1(self, url, data=None, files=None, **kwargs):
        """
        Sends a POST request using the authenticated session.
 
        :param url: The endpoint URL.
        :param data: Form data to be sent in the request body (optional).
        :param files: Files to be uploaded (optional).
        :param kwargs: Additional parameters to pass to `requests.post`.
        :return: Response object.
        """
        return self.connection.post(url, data=data, files=files, **kwargs)
 
 
    def post_data(self, endpoint, data, headers=None):
        """
        Sends a POST request to the specified endpoint with the given data and headers.
 
        :param endpoint: The API endpoint to call (relative to the host URL).
        :param data: The data payload to send with the request (typically a JSON string).
        :param headers: Optional headers to include with the request.
        :return: The response object from the POST request.
        """
        return self.connection.post(
            url=f"{self.host}/{endpoint}",
            data=data,
            headers=headers
        )
        
        
    def get_data(self, endpoint, params=None, headers=None):
        """
        Sends a GET request to the specified endpoint with the given parameters and headers.
        
        :param endpoint: The API endpoint to call (relative to the host URL).
        :param params: The query parameters to include with the GET request (typically in dictionary format).
        :param headers: Optional headers to include with the request.
        :return: The JSON response from the GET request.
        """
        try:
            # Send GET request to the specified endpoint with parameters and headers
            response = self.connection.get(
                url=f"{self.host}/{endpoint}",
                params=params,
                headers=headers
            )
            
            # Raise an exception if the request was not successful
           
            
            # Return the response as a JSON object
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data from ERP: {e}")
            return None
        
 
