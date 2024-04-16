import orjson
from twitter import search
from twitter.search import Search, get_headers
from twitter.util import find_key
from twitter.constants import *
import random
import time
from httpx import AsyncClient
from pathlib import Path
import bittensor as bt
import os
from twitter.login import Client, flow_start, flow_instrumentation, flow_username, flow_password, flow_duplication_check, init_guest_token, confirm_email

# bt.logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
#                     datefmt='%d-%b-%y %H:%M:%S',
#                     level=bt.logging.DEBUG)


PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")


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
            bt.logging.warning(f'[{RED}warning{RESET}] Please check your email for a confirmation code'
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
        proxies={'http://': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@all.dc.smartproxy.com:10000'}
    )
    client = execute_login_flow_v2(client, **kwargs)
    
    return client

class Search(Search):
    def __init__(self, email: str = None, username: str = None, password: str = None, session: search.Client = None, **kwargs):
        self.username = username
        self.save = kwargs.get('save', True)
        self.debug = kwargs.get('debug', 0)
        self.logger = self._init_logger(**kwargs)
        self.session = self._validate_session(email, username, password, session, **kwargs)
    
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
                    self.debug and bt.logging.warning(f'[{RED} ({self.username}) RATE LIMIT EXCEEDED.')# Returned {len(total)} search results for {query["query"]}{RESET}]')
                elif account_status=='locked':
                    self.debug and bt.logging.warning(f'[{RED}fail{RESET}] ({self.username}) ACCOUNT LOCKED')
                elif self.session.cookies.get('confirmation_code') == 'true' or account_status=='confirmation_code':
                    bt.logging.warning(f'[{RED}fail{RESET}] ({self.username}) CONFIRMATION CODE REQUIRED FOR LOGGING IN')
                    account_status='code_verification'
                else:
                    pass
                    # account_status = 'active'
                self.debug and bt.logging.debug(
                    f'[{GREEN}success{RESET}]({self.username}) Returned {len(total)} search results for {query["query"]}')
                return {"data": res, "rate_limit": ratelimit, "account_status": account_status}
            total |= set(find_key(entries, 'entryId'))
            self.debug and bt.logging.debug(f'({self.username}) {query["query"]}')
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
                            return data, entries, cursor, False, 'skip'
                elif self.session.cookies.get('confirmation_code') == 'true':
                    return  data, entries, cursor, False, 'code_verification'
                elif self.session.cookies.get('flow_errors') == 'true':
                    return  data, entries, cursor, False, 'skip'
                ids = set(find_key(data, 'entryId'))
                if len(ids) >= 2:
                    return data, entries, cursor, False, 'active'
            except Exception as e:
                if self.session.cookies.get('confirmation_code') == 'true':
                    return  None, [], None, False, 'code_verification'
                elif self.session.cookies.get('flow_errors') == 'true':
                    return  None, [], None, False, 'skip'
                return None, [], None, True, 'skip'
