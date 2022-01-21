import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce
import requests
import pandas as pd
from io import StringIO

load_dotenv()

api_user_salesforce_password = os.getenv('api_user_salesforce_password')
api_user_salesforce_security_token = os.getenv('api_user_salesforce_security_token')
api_user_okta_account = os.getenv('api_user_okta_account')
# reportId = '00O5G000008GDZVUA4' #new podium territory report


def get_sf_report(reportId):
    sf = Salesforce(username = api_user_okta_account, password = api_user_salesforce_password, security_token = api_user_salesforce_security_token)
    sf_instance = 'https://podium.lightning.force.com/' #Your Salesforce Instance URL
     # add report id
    export = '?isdtp=p1&export=1&enc=UTF-8&xf=csv'
    sfUrl = sf_instance + reportId + export
    response = requests.get(sfUrl, headers=sf.headers, cookies={'sid': sf.session_id})
    download_report = response.content.decode('utf-8')
    df1 = pd.read_csv(StringIO(download_report))
    return df1



