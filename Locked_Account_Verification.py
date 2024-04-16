import orjson
from twitter import search
from twitter.search import Search, get_headers
from twitter.util import find_key
from twitter.constants import *
import requests
import random
import time
from httpx import AsyncClient
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from twitter.login import  Client, flow_start, flow_instrumentation, flow_username, flow_password, flow_duplication_check, init_guest_token, confirm_email
import traceback

PROXY_USERNAME = "sp2r7fazxx"
PROXY_PASSWORD = "asaddfghjkl_"


def execute_login_flow_v2(client: Client, **kwargs) -> Client | None:
    client = init_guest_token(client)
    for fn in [flow_start, flow_instrumentation, flow_username, flow_password, flow_duplication_check]:
        client = fn(client)
    # solve email challenge
    if client.cookies.get('confirm_email') == 'true':
        client = confirm_email(client)
    # solve confirmation challenge (Proton Mail only)
    if client.cookies.get('confirmation_code') == 'true':
        if not kwargs.get('proton'):
            print(f'[{RED}warning{RESET}] Please check your email for a confirmation code'
                  f' and log in again using the web app. If you wish to automatically solve'
                  f' email confirmation challenges, add a Proton Mail account in your account settings')
            client.cookies.set('confirmation_code','true')
            # return
        # client = solve_confirmation_challenge(client, **kwargs)
    return client

def login_v2(email: str, username: str, password: str, **kwargs) -> Client:
    global PROXY_USERNAME, PROXY_PASSWORD
    client = Client(
        cookies={
            "email": email,
            "username": username,
            "password": password,
            "guest_token": None,
            "flow_token": None,
        },
        headers={
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
            'content-type': 'application/json',
            'user-agent': random.choice(USER_AGENTS),
            'x-twitter-active-user': 'yes',
            'x-twitter-client-language': 'en',
        },
        follow_redirects=True,
        proxies={'http://': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@gate.dc.smartproxy.com:20000'}
    )
    client = execute_login_flow_v2(client, **kwargs)

    
    # if not client or client.cookies.get('flow_errors') == 'true':
    #     raise Exception(f'flow_token')

    
    return client

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
                # print(f"Verifying credential: {credential['username']}")
                if credential['status'] == 'locked':
                    status = CredentialManager_V2()._check_account_status(credential)
                    if status == 'active':
                        CredentialManager_V2().release_credential(credential)
                    if status == 'skip':
                        CredentialManager_V2().release_credential(credential)
                    else:
                        CredentialManager_V2()._update_credential_database(credential, status)
                    print(f"credential {credential['username']} is {status}")
                    return credential['username'], status
                return None, None
            print("Verifying all credentials.")
            url = "http://tstempmail1.pythonanywhere.com/api/credentials/"
            response = requests.get(url)
            if response.status_code == 200:
                credentials = response.json()
                if username:
                    credentials = [credential for credential in credentials if credential['username'] == username]
            else:
                print(f"Failed to fetch credentials: {response.text}")
                credentials=[]
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_credential = {executor.submit(process_credential, credential): credential for credential in credentials}
                for future in as_completed(future_to_credential):
                    credential = future_to_credential[future]
                    try:
                        username, status = future.result()
                    except Exception as exc:
                        traceback.print_exc()
                        print(f"{credential['username']} generated an exception: {exc}")
            self.lock = 0
        else:
            print("Account verification is already in process in different thread. Skipping ..")
            time.sleep(300)


class CredentialManager_V2:
    def verify_all_credentials(self, username=None):
        def process_credential(credential):
            # print(f"Verifying credential: {credential['username']}")
            status = self._check_account_status(credential)
            if status == 'active':
                self.release_credential(credential)
            if status == 'skip':
                self.release_credential(credential)
            else:
                self._update_credential_database(credential, status)
            print(f"credential {credential['username']} is {status}")
            return credential['username'], status
        print("Verifying all credentials.")
        url = "http://tstempmail1.pythonanywhere.com/api/credentials/"
        response = requests.get(url)
        if response.status_code == 200:
            credentials = response.json()
            if username:
                credentials = [credential for credential in credentials if credential['username'] == username]
        else:
            print(f"Failed to fetch credentials: {response.text}")
            credentials=[]
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_credential = {executor.submit(process_credential, credential): credential for credential in credentials}
            for future in as_completed(future_to_credential):
                credential = future_to_credential[future]
                try:
                    username, status = future.result()
                except Exception as exc:
                    print(f"{credential['username']} generated an exception: {exc}")
        
        # print("Verifying identity verification credentials.")
        # url = "http://tstempmail1.pythonanywhere.com/api/credentials/"
        # response = requests.get(url)
        # if response.status_code == 200:
        #     credentials = response.json()
        #     if username:
        #         credentials = [credential for credential in credentials if credential['username'] == username and credential['status']=='email_verification']
        # else:
        #     print(f"Failed to fetch credentials: {response.text}")
        #     credentials=[]
        
        # with ThreadPoolExecutor(max_workers=10) as executor:
        #     future_to_credential = {executor.submit(process_credential, credential): credential for credential in credentials}
        #     for future in as_completed(future_to_credential):
        #         credential = future_to_credential[future]
        #         try:
        #             username, status = future.result()
        #         except Exception as exc:
        #             print(f"{credential['username']} generated an exception: {exc}")



    def request_credential(self, _type = 'debug'):
        try:
            response = requests.get(f"http://tstempmail1.pythonanywhere.com/api/lock_credential/?ctype={_type}")
            if response.status_code == 200:
                self.credential = response.json()
                status = self._check_account_status(self.credential)
                if status == 'active':
                    print(f"Fetched Credential: {self.credential['username']}")
                    return self.credential
                elif status == 'skip':
                    # print(f"There is some issue while fetching credential {self.credential['username']}. Skipping and fetching different one .. ")
                    return None
                else:
                    self.release_credential(self.credential)
                    self._update_credential_database(self.credential, status)
                    return None
            else:
                if 'No credentials available' in response.text:
                    requests.get('http://tstempmail1.pythonanywhere.com/api/activate_all/')
                    CredentialVerification().verify_all_credentials()
                    
                print(f"Error while fetching credentials: {response.text}")
                return  None
        except requests.RequestException as e:
            print(f"Failed to fetch credentials: {e}")


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
            print(f"({credential['username']}) Exception - {ex}")
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
            update_url = f"http://tstempmail1.pythonanywhere.com/api/credentials/{credential['id']}/"
            payload = {'status': status}
            response = requests.patch(update_url, json=payload)
            response.raise_for_status()
            print(f"Updated {credential['username']} status to {status}.")
        except requests.RequestException as e:
            print(f"Failed to update credential {credential['id']}: {e}")

    def release_credential(self, credential):
        """Function to unlock a specific credential using the API."""
        response = requests.post(f"http://tstempmail1.pythonanywhere.com/api/unlock_credential/{credential['username']}/")
        if response.status_code == 200:
            print(f"Credential {credential['username']} released.")
        else:
            print(f"Failed to release credential {credential['username']} - {response.text}") 
    
    def handle_locked_account(self, credential):
        self._update_credential_database(credential, 'locked')
        print(f"Removed and updated locked account: {credential['username']}")

class CredentialManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(CredentialManager, cls).__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.credentials = []
        self.credential_locks = {}
        self.access_lock = threading.Condition()
        self._get_and_filter_credentials()

    def _get_and_filter_credentials(self):
        try:
            response = requests.get("http://tstempmail1.pythonanywhere.com/api/credentials/")
            response.raise_for_status()
            fetched_credentials = response.json()
            active_credentials = [cred for cred in fetched_credentials if not self._is_locked(cred)]
            with self.access_lock:
                self.credentials = active_credentials
                self.credential_locks = {cred['id']: threading.Lock() for cred in self.credentials}
                print(f"Fetched {len(self.credentials)} active credentials from database.")
        except requests.RequestException as e:
            print(f"Failed to fetch credentials: {e}")

    def _is_locked(self, credential):
        return self._check_account_status(credential)

    def request_credential(self, _type = 'debug'):
        with self.access_lock:
            while not self.credentials:
                return None
            for credential in [c for c in self.credentials if c['ctype'] == _type]:
                if self._check_account_status(credential):
                    self.handle_locked_account(credential)
                lock = self.credential_locks[credential['id']]
                if lock.acquire(blocking=False):
                    self.credentials.remove(credential)
                    return credential
            return None

    def handle_locked_account(self, credential):
        with self.access_lock:
            if credential in self.credentials: 
                self.credentials.remove(credential)
                self._update_credential_database(credential, 'locked')
                print(f"Removed and updated locked account: {credential['username']}")


    def release_credential(self, credential):
        with self.access_lock:
            self.credentials.append(credential)
            self.credential_locks[credential['id']].release()
            self.access_lock.notifyAll()

    # Assuming this function checks the real-time status of the account and updates the database if locked
    def _check_account_status(self, credential, retries=3):
        if retries == 0:
            return True
        try:
            results = Search(credential['email'], credential['username'], credential['password'], save=False, debug=0).run(
                limit=1,
                retries=1,
                queries=[
                    {
                        'category': 'Latest',
                        'query': 'covid'
                    }
                ]
            )[0]
        except:
            retries -= 1
            return self._check_account_status(credential, retries)
        if results['account_status']:
            return True
        return False

    def _update_credential_database(self, credential, status):
        try:
            update_url = f"http://tstempmail1.pythonanywhere.com/api/credentials/{credential['id']}/"
            payload = {'status': status}
            response = requests.patch(update_url, json=payload)
            response.raise_for_status()
            print(f"Updated {credential['username']} status to {status}.")
        except requests.RequestException as e:
            print(f"Failed to update credential {credential['id']}: {e}")

class Search(Search):
    def __init__(self, email: str = None, username: str = None, password: str = None, session: search.Client = None, **kwargs):
        self.username = username
        # super().__init__(email, username, password, session, **kwargs)
        self.save = kwargs.get('save', True)
        self.debug = kwargs.get('debug', 0)
        self.logger = self._init_logger(**kwargs)
        self.session = self._validate_session(email, username, password, session, **kwargs)
        # print(f'session - {self.session}')
        # print(self.session.cookies.get('confirmation_code'))
    
    @staticmethod
    def _validate_session(*args, **kwargs):
        email, username, password, session = args

        # validate credentials
        if all((email, username, password)):
            return login_v2(email, username, password, **kwargs)

        # invalid credentials, try validating session
        if session and all(session.cookies.get(c) for c in {'ct0', 'auth_token'}):
            return session

        # invalid credentials and session
        cookies = kwargs.get('cookies')

        # try validating cookies dict
        if isinstance(cookies, dict) and all(cookies.get(c) for c in {'ct0', 'auth_token'}):
            _session = Client(cookies=cookies, follow_redirects=True)
            _session.headers.update(get_headers(_session))
            return _session

        # try validating cookies from file
        if isinstance(cookies, str):
            _session = Client(cookies=orjson.loads(Path(cookies).read_bytes()), follow_redirects=True)
            _session.headers.update(get_headers(_session))
            return _session

        raise Exception('Session not authenticated. '
                        'Please use an authenticated session or remove the `session` argument and try again.')

    async def paginate(self, client: AsyncClient, query: dict, limit: int, out: Path, **kwargs) -> list[dict]:
        params = {
            'variables': {
                'count': 20,
                'querySource': 'typed_query',
                'rawQuery': query['query'],
                'product': query['category']
            },
            'features': Operation.default_features,
            'fieldToggles': {'withArticleRichContentState': False},
        }

        res = []
        cursor = ''
        total = set()
        while True:
            if cursor:
                params['variables']['cursor'] = cursor
            data, entries, cursor, ratelimit, account_status = await self.backoff(lambda: self.get(client, params), **kwargs)
            res.extend(entries)
            if len(entries) <= 2 or len(total) >= limit or ratelimit or account_status!='active':  # just cursors
                if ratelimit:
                    self.debug and print(f'[{RED} ({self.username}) RATE LIMIT EXCEEDED.')# Returned {len(total)} search results for {query["query"]}{RESET}]')
                elif account_status=='locked':
                    self.debug and print(f'[{RED}fail{RESET}] ({self.username}) ACCOUNT LOCKED')
                elif self.session.cookies.get('confirmation_code') == 'true' or account_status=='confirmation_code':
                    print(f'[{RED}fail{RESET}] ({self.username}) CONFIRMATION CODE REQUIRED FOR LOGGING IN')
                    account_status='code_verification'
                # elif self.session.cookies.get('confirm_email') == 'true' or account_status=='email_verification':
                #     print(f'[{RED}fail{RESET}] ({self.username}) IDENTITY VERIFICATION REQUIRED')
                #     account_status = 'email_verification'
                else:
                    account_status = 'active'
                self.debug and print(
                    f'[{GREEN}success{RESET}]({self.username}) Returned {len(total)} search results for {query["query"]}')
                return {"data": res, "rate_limit": ratelimit, "account_status": account_status}
            total |= set(find_key(entries, 'entryId'))
            self.debug and print(f'({self.username}) {query["query"]}')
            self.save and (out / f'{time.time_ns()}.json').write_bytes(orjson.dumps(entries))


    async def backoff(self, fn, **kwargs):
        retries = kwargs.get('retries', 3)
        entries = []
        for i in range(retries + 1):
            try:
                data, entries, cursor = await fn()
                if errors := data.get('errors'):
                    for e in errors:
                        if 'account is temporarily locked' in e.get('message'):
                            return data, entries, cursor, False, 'locked'
                        else:
                            # print(f'({self.username}) {e.get("message")}')
                            return data, entries, cursor, False, 'invalid_credential'
                elif self.session.cookies.get('confirmation_code') == 'true':
                    return  data, entries, cursor, False, 'code_verification'
                # elif self.session.cookies.get('confirm_email') == 'true':
                #     return  data, entries, cursor, False, 'email_verification'
                elif self.session.cookies.get('flow_errors') == 'true':
                    return  data, entries, cursor, False, 'invalid_credential'
                ids = set(find_key(data, 'entryId'))
                if len(ids) >= 2:
                    return data, entries, cursor, False, 'active'
            except Exception as e:
                if self.session.cookies.get('confirmation_code') == 'true':
                    return  None, [], None, False, 'code_verification'
                # elif self.session.cookies.get('confirm_email') == 'true':
                #     return  None, [], None, False, 'email_verification'
                elif self.session.cookies.get('flow_errors') == 'true':
                    return  None, [], None, False, 'invalid_credential'
                return None, [], None, True, 'active'


if __name__ =='__main__':
    CredentialVerification().verify_all_credentials()