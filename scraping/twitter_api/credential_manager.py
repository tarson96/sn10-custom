from scraping.twitter_api.twitter_api import Search
from twitter.constants import *
import requests
import time
import bittensor as bt
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

class CredentialVerification(object):
    def __init__(self):
        self.lock = 0

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CredentialVerification, cls).__new__(cls)
            return cls.instance
  
    def verify_all_credentials(self, username=None):
        if not self.lock:
            self.lock = 1
            def process_credential(credential):
                # bt.logging.info(f"Verifying credential: {credential['username']}")
                status = CredentialManager()._check_account_status(credential)
                if status == 'active':
                    CredentialManager().release_credential(credential)
                if status == 'skip':
                    CredentialManager().release_credential(credential)
                else:
                    CredentialManager()._update_credential_database(credential, status)
                bt.logging.info(f"credential {credential['username']} is {status}")
                return credential['username'], status
            bt.logging.info("Verifying all credentials.")
            url = "http://tstempmail1.pythonanywhere.com/api/credentials/"
            response = requests.get(url)
            if response.status_code == 200:
                credentials = response.json()
                if username:
                    credentials = [credential for credential in credentials if credential['username'] == username]
            else:
                bt.logging.warning(f"Failed to fetch credentials: {response.text}")
                credentials=[]
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_credential = {executor.submit(process_credential, credential): credential for credential in credentials}
                for future in as_completed(future_to_credential):
                    credential = future_to_credential[future]
                    try:
                        username, status = future.result()
                    except Exception as exc:
                        bt.logging.error(f"{credential['username']} generated an exception: {exc}")
            self.lock = 0
        else:
            bt.logging.info("Account verification is already in process in different thread. Skipping ..")
            time.sleep(300)

class CredentialManager:
    def verify_all_credentials(self, username=None):
        def process_credential(credential):
            status = self._check_account_status(credential)
            if status == 'active':
                self.release_credential(credential)
            if status == 'skip':
                self.release_credential(credential)
            else:
                self._update_credential_database(credential, status)
            bt.logging.info(f"credential {credential['username']} is {status}")
            return credential['username'], status
        bt.logging.info("Verifying all credentials.")
        url = "http://tstempmail1.pythonanywhere.com/api/credentials/"
        response = requests.get(url)
        if response.status_code == 200:
            credentials = response.json()
            if username:
                credentials = [credential for credential in credentials if credential['username'] == username]
        else:
            bt.logging.warning(f"Failed to fetch credentials: {response.text}")
            credentials=[]
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_credential = {executor.submit(process_credential, credential): credential for credential in credentials}
            for future in as_completed(future_to_credential):
                credential = future_to_credential[future]
                try:
                    username, status = future.result()
                except Exception as exc:
                    bt.logging.error(f"{credential['username']} generated an exception: {exc}")
        
        # bt.logging.info("Verifying identity verification credentials.")
        # url = "http://tstempmail1.pythonanywhere.com/api/credentials/"
        # response = requests.get(url)
        # if response.status_code == 200:
        #     credentials = response.json()
        #     if username:
        #         credentials = [credential for credential in credentials if credential['username'] == username and credential['status']=='email_verification']
        # else:
        #     bt.logging.error(f"Failed to fetch credentials: {response.text}")
        #     credentials=[]
        
        # with ThreadPoolExecutor(max_workers=10) as executor:
        #     future_to_credential = {executor.submit(process_credential, credential): credential for credential in credentials}
        #     for future in as_completed(future_to_credential):
        #         credential = future_to_credential[future]
        #         try:
        #             username, status = future.result()
        #         except Exception as exc:
        #             bt.logging.error(f"{credential['username']} generated an exception: {exc}")

    def request_credential(self, _type = 'debug'):
        try:
            response = requests.get(f"http://tstempmail1.pythonanywhere.com/api/lock_credential/?ctype={_type}")
            if response.status_code == 200:
                self.credential = response.json()
                status = self._check_account_status(self.credential)
                if status == 'active':
                    bt.logging.info(f"Fetched Credential: {self.credential['username']}")
                    return self.credential
                elif status == 'skip':
                    # bt.logging.info(f"There is some issue while fetching credential {self.credential['username']}. Skipping and fetching different one .. ")
                    return None
                else:
                    self.release_credential(self.credential)
                    self._update_credential_database(self.credential, status)
                    return None
            else:
                if 'No credentials available' in response.text:
                    requests.get('http://tstempmail1.pythonanywhere.com/api/activate_all/')
                    CredentialVerification().verify_all_credentials()
                    
                bt.logging.warning(f"Error while fetching credentials: {response.text}")
                return  None
        except requests.RequestException as e:
            bt.logging.error(f"Failed to fetch credentials: {e}")

    def _check_account_status(self, credential, retries=3):
        if retries == 0:
            return 'skip'
        try:
            results = Search(credential['email'], credential['username'], credential['password'], save=False, debug=0).run(
                limit=1,
                retries=1,
                queries=[
                    {
                        'category': 'Latest',
                        'query': 'e'
                    }
                ]
            )[0]
        except Exception as ex:
            bt.logging.error(f"({credential['username']}) Exception - {ex}")
            # print(ex.with_traceback())
            # if 'confirm_email' in str(ex):
            #     return 'email_verification'
            if 'confirmation code' in str(ex):
                return 'code_verification'
            elif 'flow_token' in str(ex):
                return 'skip'
            else:
                retries -= 1
                return self._check_account_status(credential, retries)
        if results['account_status'] != 'active':
            return results['account_status']
        else:
            if credential['status'] != 'active':
                self._update_credential_database(credential, 'active')
        return 'active'

    def _update_credential_database(self, credential, status):
        try:
            if status in ['active', 'locked', 'code_verification']:
                update_url = f"http://tstempmail1.pythonanywhere.com/api/credentials/{credential['id']}/"
                payload = {'status': status}
                response = requests.patch(update_url, json=payload)
                response.raise_for_status()
                bt.logging.info(f"Updated {credential['username']} status to {status}.")
        except requests.RequestException as e:
            bt.logging.error(f"Failed to update credential {credential['id']}: {e}")

    def release_credential(self, credential):
        """Function to unlock a specific credential using the API."""
        response = requests.post(f"http://tstempmail1.pythonanywhere.com/api/unlock_credential/{credential['username']}/")
        if response.status_code == 200:
            bt.logging.info(f"Credential {credential['username']} released.")
        else:
            bt.logging.info(f"Failed to release credential {credential['username']} - {response.text}") 
    
    def handle_locked_account(self, credential):
        self._update_credential_database(credential, 'locked')
        bt.logging.info(f"Removed and updated locked account: {credential['username']}")
