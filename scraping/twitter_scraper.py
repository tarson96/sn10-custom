from twitter import scraper
from twitter.constants import *
import requests
import random
import time
from datetime import datetime, timedelta, timezone
import bittensor as bt
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json
import traceback
import string
import os
import re
import sqlite3
from scraping.twitter_api.twitter_api import Search
from scraping.twitter_api.credential_manager import CredentialManager

# bt.logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
#                     datefmt='%d-%b-%y %H:%M:%S',
#                     level=bt.logging.DEBUG)


class TwitterScraper:
    def __init__(self, limit=None, since_date=None, until_date=None, labels = None, uri=None, run_type='debug'):
        self.since_date = since_date
        self.until_date = until_date
        self.labels = labels
        self.uri = uri
        self.limit = limit
        self.since_id = None
        self.used_credentials = []
        self.blocked_credentials = []
        self.total_tweets = self.limit
        self.max_workers = 2
        self.run_type = run_type

    def query_generator(self, labels: list, since_date: datetime, until_date: datetime, since_id: str=None):
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"
        query = ''
        if since_date:
            query += f'since:{since_date.strftime(date_format)} '
        if until_date:
            query += f'until:{until_date.strftime(date_format)} '
        if labels:
            label_query = " OR ".join([label for label in labels])
            query += f" ({label_query})"
        else:
            query += f" filter:hashtags ({' OR '.join(random.sample(string.ascii_letters, 3))})"
        if since_id:
            query = f"since_id:{since_id} " + query
        bt.logging.info(f"Generated query: {query}")
        return query

    def is_ratelimit_execeeded(self, credential):
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
        if results['rate_limit']:
            return True
        return False    

    def get_since_date_and_id(self, results):
        if 'tweet' in results['data'][-1]['content']['itemContent']['tweet_results']['result']:
            since_date = datetime.strptime(results['data'][-1]['content']['itemContent']['tweet_results']['result']['tweet']['legacy']["created_at"], "%a %b %d %H:%M:%S %z %Y")
            since_id = results['data'][-1]['content']['itemContent']['tweet_results']['result']['tweet']['legacy']["id_str"]
        else:
            since_date = datetime.strptime(results['data'][-1]['content']['itemContent']['tweet_results']['result']['legacy']["created_at"], "%a %b %d %H:%M:%S %z %Y")
            since_id = results['data'][-1]['content']['itemContent']['tweet_results']['result']['legacy']["id_str"]
        return since_date, since_id

    def search(self):
        data = []
        i = 0
        retries = 0
        while True:
            if retries < 5:
                credential = CredentialManager().request_credential(self.run_type)
                if not credential:
                    i+=1
                    t = 2 + i + random.random()
                    bt.logging.warning(f" No available credentials. Sleeping for {t} seconds.")
                    time.sleep(t)
                    continue
                if self.is_ratelimit_execeeded(credential):
                    CredentialManager().release_credential(credential)
                    i+=1
                    t = 2 + i + random.random()
                    bt.logging.warning(f" Rate limit exceeded for username  {credential['username']}. Sleeping for {t} seconds.")
                    time.sleep(t)
                    continue
                i=1
                bt.logging.info(f"Using username {credential['username']}.")
                try:
                    sc = Search(credential['email'], credential['username'], credential['password'], save=False, debug=1)
                    results = sc.run(
                        limit=self.limit,
                        retries=self.limit,
                        queries=[
                            {
                                'category': 'Latest',
                                'query': self.query_generator(self.labels, self.since_date, self.until_date, self.since_id)
                            }
                        ]
                    )[0]
                except Exception as ex:
                    traceback.print_exc()
                    bt.logging.error(f" Error occurred while scraping: {ex}")
                    CredentialManager().release_credential(credential)
                    retries += 1
                    continue
                CredentialManager().release_credential(credential)
                data.extend(results['data'])
                bt.logging.info(f" Scraped {len(results['data'])} tweets using username {credential['username']}. Total tweets scraped = {len(data)}")
                if results['account_status'] != 'active':
                    try:
                        if results['data']:
                            self.total_tweets -= len(results['data'])
                            self.limit -= len(results['data'])
                            self.since_date, self.since_id = self.get_since_date_and_id(results)
                            retries = 0
                    except Exception as ex:
                        bt.logging.error(f"Error while handling rate-limitation: {ex}")
                        retries+=1
                    bt.logging.warning(f" Account {credential['username']} is locked. Removing from credentials database.")
                    CredentialManager()._update_credential_database(credential, results['account_status'])
                    CredentialManager().release_credential(credential)
                    continue
                elif results['rate_limit']:
                    try:
                        if results['data']:
                            self.total_tweets -= len(results['data'])
                            self.limit -= len(results['data'])
                            self.since_date, self.since_id = self.get_since_date_and_id(results)
                            retries = 0
                    except Exception as ex:
                        bt.logging.error(f"Error while handling rate-limitation: {ex}")
                        retries+=1
                    continue
                else:
                    retries = 0
                    return data
            else:
                bt.logging.error(f"Max retries exceeded for query - {self.query_generator(self.labels, self.since_date, self.until_date, self.since_id)}. Returning {len(data)} tweet data.")
                return data

    def get_trending_hashtags(self):
        i = 1
        retries = 0
        result = []
        while True:
            if retries<5:
                credential = CredentialManager().request_credential(self.run_type)
                if not credential:
                    i+=1
                    t = 2 ** i + random.random()
                    bt.logging.warning(f" No available credentials. Sleeping for {t} seconds.")
                    time.sleep(t)
                    continue
                if self.is_ratelimit_execeeded(credential):
                    CredentialManager().release_credential(credential)
                    i+=1
                    t = 2 ** i + random.random()
                    bt.logging.warning(f" Rate limit exceeded for username  {credential['username']}. Sleeping for {t} seconds.")
                    time.sleep(t)
                    continue
                i=1
                bt.logging.info(f"Using username {credential['username']} for fetching trending_hashtag.")
                try:
                    sc = scraper.Scraper(credential['email'], credential['username'], credential['password'], save=False, debug=0)
                    result = sc.trends(["-1200", "-1100", "-1000", "-0900", "-0800", "-0700", "-0600", "-0500", "-0400", "-0300",
                              "-0200", "-0100", "+0000", "+0100", "+0200", "+0300", "+0400", "+0500", "+0600", "+0700",
                              "+0800", "+0900", "+1000", "+1100", "+1200", "+1300", "+1400"])
                except:
                    CredentialManager().release_credential(credential)
                    retries += 1
                    continue
                CredentialManager().release_credential(credential)
            return result

def get_last_tweettime_for_hashtag(hashtag:str):
    
    try:
        conn = sqlite3.connect('SqliteMinerStorage.sqlite')
        cursor = conn.cursor()
        cursor.execute(f"SELECT max(datetime) as last_scraped_datetime FROM DataEntity WHERE label='{hashtag.lower()}' and datetime>DATE('now','-1 day')")
        results = cursor.fetchall()
        cursor.close()
        if results:
            since_date = results[0][0]
            since_date = datetime.strptime(since_date, '%Y-%m-%d %H:%M:%S%z')
        else:
            since_date = None
    except Exception as ex:
        # traceback.print_exc()
        bt.logging.error(f'Error while generating since_id for {hashtag}: {ex}')
        since_date = None
    return since_date

def get_all_trending_hashtags():
    if os.path.exists('trending_hashtags.json') and datetime.strptime(json.loads(open('trending_hashtags.json','r').read())['load_date'], '%Y-%m-%d %H:%M:%S') > (datetime.now() - timedelta(hours=3)):
        return json.loads(open('trending_hashtags.json','r').read())['hashtags']
    trends_in = ['/', '/algeria/', '/algeria/algiers/', '/argentina/', '/argentina/buenos-aires/', '/argentina/cordoba/', '/argentina/mendoza/', '/argentina/rosario/', '/australia/', '/australia/adelaide/', '/australia/brisbane/', '/australia/canberra/', '/australia/darwin/', '/australia/melbourne/', '/australia/perth/', '/australia/sydney/', '/austria/', '/austria/vienna/', '/bahrain/', '/belarus/', '/belarus/brest/', '/belarus/gomel/', '/belarus/grodno/', '/belarus/minsk/', '/belgium/', '/brazil/', '/brazil/belem/', '/brazil/belo-horizonte/', '/brazil/brasilia/', '/brazil/campinas/', '/brazil/curitiba/', '/brazil/fortaleza/', '/brazil/goiania/', '/brazil/guarulhos/', '/brazil/manaus/', '/brazil/porto-alegre/', '/brazil/recife/', '/brazil/rio-de-janeiro/', '/brazil/salvador/', '/brazil/sao-luis/', '/brazil/sao-paulo/', '/canada/', '/canada/calgary/', '/canada/edmonton/', '/canada/montreal/', '/canada/ottawa/', '/canada/quebec/', '/canada/toronto/', '/canada/vancouver/', '/canada/winnipeg/', '/chile/', '/chile/concepcion/', '/chile/santiago/', '/chile/valparaiso/', '/colombia/', '/colombia/barranquilla/', '/colombia/bogota/', '/colombia/cali/', '/colombia/medellin/', '/denmark/', '/dominican-republic/', '/dominican-republic/santo-domingo/', '/ecuador/', '/ecuador/guayaquil/', '/ecuador/quito/', '/egypt/', '/egypt/alexandria/', '/egypt/cairo/', '/egypt/giza/', '/france/', '/france/bordeaux/', '/france/lille/', '/france/lyon/', '/france/marseille/', '/france/montpellier/', '/france/nantes/', '/france/paris/', '/france/rennes/', '/france/strasbourg/', '/france/toulouse/', '/germany/', '/germany/berlin/', '/germany/bremen/', '/germany/cologne/', '/germany/dortmund/', '/germany/dresden/', '/germany/dusseldorf/', '/germany/essen/', '/germany/frankfurt/', '/germany/hamburg/', '/germany/leipzig/', '/germany/munich/', '/germany/stuttgart/', '/ghana/', '/ghana/accra/', '/ghana/kumasi/', '/greece/', '/greece/athens/', '/greece/thessaloniki/', '/guatemala/', '/guatemala/guatemala-city/', '/india/', '/india/ahmedabad/', '/india/amritsar/', '/india/bangalore/', '/india/bhopal/', '/india/chennai/', '/india/delhi/', '/india/hyderabad/', '/india/indore/', '/india/jaipur/', '/india/kanpur/', '/india/kolkata/', '/india/lucknow/', '/india/mumbai/', '/india/nagpur/', '/india/patna/', '/india/pune/', '/india/rajkot/', '/india/ranchi/', '/india/srinagar/', '/india/surat/', '/india/thane/', '/indonesia/', '/indonesia/bandung/', '/indonesia/bekasi/', '/indonesia/depok/', '/indonesia/jakarta/', '/indonesia/makassar/', '/indonesia/medan/', '/indonesia/palembang/', '/indonesia/pekanbaru/', '/indonesia/semarang/', '/indonesia/surabaya/', '/indonesia/tangerang/', '/ireland/', '/ireland/cork/', '/ireland/dublin/', '/ireland/galway/', '/israel/', '/israel/haifa/', '/israel/jerusalem/', '/israel/tel-aviv/', '/italy/', '/italy/bologna/', '/italy/genoa/', '/italy/milan/', '/italy/naples/', '/italy/palermo/', '/italy/rome/', '/italy/turin/', '/japan/', '/japan/chiba/', '/japan/fukuoka/', '/japan/hamamatsu/', '/japan/hiroshima/', '/japan/kawasaki/', '/japan/kitakyushu/', '/japan/kobe/', '/japan/kumamoto/', '/japan/kyoto/', '/japan/nagoya/', '/japan/niigata/', '/japan/okayama/', '/japan/okinawa/', '/japan/osaka/', '/japan/sagamihara/', '/japan/saitama/', '/japan/sapporo/', '/japan/sendai/', '/japan/takamatsu/', '/japan/tokyo/', '/japan/yokohama/', '/jordan/', '/jordan/amman/', '/kenya/', '/kenya/mombasa/', '/kenya/nairobi/', '/korea/', '/korea/ansan/', '/korea/bucheon/', '/korea/busan/', '/korea/changwon/', '/korea/daegu/', '/korea/daejeon/', '/korea/goyang/', '/korea/gwangju/', '/korea/incheon/', '/korea/seongnam/', '/korea/seoul/', '/korea/suwon/', '/korea/ulsan/', '/korea/yongin/', '/kuwait/', '/latvia/', '/latvia/riga/', '/lebanon/', '/malaysia/', '/malaysia/hulu-langat/', '/malaysia/ipoh/', '/malaysia/johor-bahru/', '/malaysia/kajang/', '/malaysia/klang/', '/malaysia/kuala-lumpur/', '/malaysia/petaling/', '/mexico/', '/mexico/acapulco/', '/mexico/aguascalientes/', '/mexico/chihuahua/', '/mexico/ciudad-juarez/', '/mexico/culiacan/', '/mexico/ecatepec-de-morelos/', '/mexico/guadalajara/', '/mexico/hermosillo/', '/mexico/leon/', '/mexico/merida/', '/mexico/mexicali/', '/mexico/mexico-city/', '/mexico/monterrey/', '/mexico/morelia/', '/mexico/naucalpan-de-juarez/', '/mexico/nezahualcoyotl/', '/mexico/puebla/', '/mexico/queretaro/', '/mexico/saltillo/', '/mexico/san-luis-potosi/', '/mexico/tijuana/', '/mexico/toluca/', '/mexico/zapopan/', '/netherlands/', '/netherlands/amsterdam/', '/netherlands/den-haag/', '/netherlands/rotterdam/', '/netherlands/utrecht/', '/new-zealand/', '/new-zealand/auckland/', '/nigeria/', '/nigeria/benin-city/', '/nigeria/ibadan/', '/nigeria/kaduna/', '/nigeria/kano/', '/nigeria/lagos/', '/nigeria/port-harcourt/', '/norway/', '/norway/bergen/', '/norway/oslo/', '/oman/', '/oman/muscat/', '/pakistan/', '/pakistan/faisalabad/', '/pakistan/karachi/', '/pakistan/lahore/', '/pakistan/multan/', '/pakistan/rawalpindi/', '/panama/', '/peru/', '/peru/lima/', '/philippines/', '/philippines/antipolo/', '/philippines/cagayan-de-oro/', '/philippines/calocan/', '/philippines/cebu-city/', '/philippines/davao-city/', '/philippines/makati/', '/philippines/manila/', '/philippines/pasig/', '/philippines/quezon-city/', '/philippines/taguig/', '/philippines/zamboanga-city/', '/poland/', '/poland/gdansk/', '/poland/krakow/', '/poland/lodz/', '/poland/poznan/', '/poland/warsaw/', '/poland/wroclaw/', '/portugal/', '/puerto-rico/', '/qatar/', '/russia/', '/russia/chelyabinsk/', '/russia/irkutsk/', '/russia/kazan/', '/russia/khabarovsk/', '/russia/krasnodar/', '/russia/krasnoyarsk/', '/russia/moscow/', '/russia/nizhny-novgorod/', '/russia/novosibirsk/', '/russia/omsk/', '/russia/perm/', '/russia/rostov-on-don/', '/russia/saint-petersburg/', '/russia/samara/', '/russia/ufa/', '/russia/vladivostok/', '/russia/volgograd/', '/russia/voronezh/', '/russia/yekaterinburg/', '/saudi-arabia/', '/saudi-arabia/ahsa/', '/saudi-arabia/dammam/', '/saudi-arabia/jeddah/', '/saudi-arabia/mecca/', '/saudi-arabia/medina/', '/saudi-arabia/riyadh/', '/singapore/', '/singapore/', '/south-africa/', '/south-africa/cape-town/', '/south-africa/durban/', '/south-africa/johannesburg/', '/south-africa/port-elizabeth/', '/south-africa/pretoria/', '/south-africa/soweto/', '/spain/', '/spain/barcelona/', '/spain/bilbao/', '/spain/las-palmas/', '/spain/madrid/', '/spain/malaga/', '/spain/murcia/', '/spain/palma/', '/spain/seville/', '/spain/valencia/', '/spain/zaragoza/', '/sweden/', '/sweden/gothenburg/', '/sweden/stockholm/', '/switzerland/', '/switzerland/geneva/', '/switzerland/lausanne/', '/switzerland/zurich/', '/thailand/', '/thailand/bangkok/', '/turkey/', '/turkey/adana/', '/turkey/ankara/', '/turkey/antalya/', '/turkey/bursa/', '/turkey/diyarbakır/', '/turkey/eskisehir/', '/turkey/gaziantep/', '/turkey/istanbul/', '/turkey/izmir/', '/turkey/kayseri/', '/turkey/konya/', '/turkey/mersin/', '/ukraine/', '/ukraine/dnipropetrovsk/', '/ukraine/donetsk/', '/ukraine/kharkiv/', '/ukraine/kyiv/', '/ukraine/lviv/', '/ukraine/odesa/', '/ukraine/zaporozhye/', '/united-arab-emirates/', '/united-arab-emirates/abu-dhabi/', '/united-arab-emirates/dubai/', '/united-arab-emirates/sharjah/', '/united-kingdom/', '/united-kingdom/belfast/', '/united-kingdom/birmingham/', '/united-kingdom/blackpool/', '/united-kingdom/bournemouth/', '/united-kingdom/brighton/', '/united-kingdom/bristol/', '/united-kingdom/cardiff/', '/united-kingdom/coventry/', '/united-kingdom/derby/', '/united-kingdom/edinburgh/', '/united-kingdom/glasgow/', '/united-kingdom/hull/', '/united-kingdom/leeds/', '/united-kingdom/leicester/', '/united-kingdom/liverpool/', '/united-kingdom/london/', '/united-kingdom/manchester/', '/united-kingdom/middlesbrough/', '/united-kingdom/newcastle/', '/united-kingdom/nottingham/', '/united-kingdom/plymouth/', '/united-kingdom/portsmouth/', '/united-kingdom/preston/', '/united-kingdom/sheffield/', '/united-kingdom/stoke-on-trent/', '/united-kingdom/swansea/', '/united-states/', '/united-states/albuquerque/', '/united-states/atlanta/', '/united-states/austin/', '/united-states/baltimore/', '/united-states/baton-rouge/', '/united-states/birmingham/', '/united-states/boston/', '/united-states/charlotte/', '/united-states/chicago/', '/united-states/cincinnati/', '/united-states/cleveland/', '/united-states/colorado-springs/', '/united-states/columbus/', '/united-states/dallas-ft-worth/', '/united-states/denver/', '/united-states/detroit/', '/united-states/el-paso/', '/united-states/fresno/', '/united-states/greensboro/', '/united-states/harrisburg/', '/united-states/honolulu/', '/united-states/houston/', '/united-states/indianapolis/', '/united-states/jackson/', '/united-states/jacksonville/', '/united-states/kansas-city/', '/united-states/las-vegas/', '/united-states/long-beach/', '/united-states/los-angeles/', '/united-states/louisville/', '/united-states/memphis/', '/united-states/mesa/', '/united-states/miami/', '/united-states/milwaukee/', '/united-states/minneapolis/', '/united-states/nashville/', '/united-states/new-haven/', '/united-states/new-orleans/', '/united-states/new-york/', '/united-states/norfolk/', '/united-states/oklahoma-city/', '/united-states/omaha/', '/united-states/orlando/', '/united-states/philadelphia/', '/united-states/phoenix/', '/united-states/pittsburgh/', '/united-states/portland/', '/united-states/providence/', '/united-states/raleigh/', '/united-states/richmond/', '/united-states/sacramento/', '/united-states/salt-lake-city/', '/united-states/san-antonio/', '/united-states/san-diego/', '/united-states/san-francisco/', '/united-states/san-jose/', '/united-states/seattle/', '/united-states/st-louis/', '/united-states/tallahassee/', '/united-states/tampa/', '/united-states/tucson/', '/united-states/virginia-beach/', '/united-states/washington/', '/venezuela/', '/venezuela/barcelona/', '/venezuela/barquisimeto/', '/venezuela/caracas/', '/venezuela/ciudad-guayana/', '/venezuela/maracaibo/', '/venezuela/maracay/', '/venezuela/maturin/', '/venezuela/turmero/', '/venezuela/valencia/', '/vietnam/', '/vietnam/can-tho/', '/vietnam/da-nang/', '/vietnam/hai-phong/', '/vietnam/hanoi/', '/vietnam/ho-chi-minh-city/']
    trends = TwitterScraper().get_trending_hashtags()
    hashtags = []
    json.dump(trends, open('trends.json','w'))
    for country in trends:
        trend = country.keys()
        hashtags.extend([h for h in trend if '#' in h])

    bt.logging.info('Fetching trending hashtags from getdaytrends.com')
    for i in range(1,16):
        headers  = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.7',
            'cache-control': 'max-age=0',
            'sec-ch-ua': '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'sec-gpc': '1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        }
        response = requests.get(f'https://getdaytrends.com/{i}/', headers=headers)
        if response.status_code == 200:
            for hashtag in re.findall(r'(\#[\S]+)\<\/a\>', response.text):
                if '#' in hashtag:
                    hashtags.append(hashtag)
    
    bt.logging.info('Fetching trending hashtags from trend.in')
    def get_trending_hashtags_trendin(url):
        response = requests.get(url)
        if response.status_code == 200:
            return re.findall(r'(\#[\S]+)\<\/a\>', response.text)
        return []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_url = {executor.submit(get_trending_hashtags_trendin, f"https://trends24.in{url}"): url for url in trends_in}
        for future in as_completed(future_to_url):
            hashtags.extend(future.result())

    hashtags = list(set(hashtags))
    json.dump({"load_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S%z'), "hashtags": hashtags}, open('trending_hashtags.json', 'w'))
    return hashtags
  
def get_top_trends24_hashtag():
    from bs4 import BeautifulSoup
    trends_in = ['/', '/algeria/', '/algeria/algiers/', '/argentina/', '/argentina/buenos-aires/', '/argentina/cordoba/', '/argentina/mendoza/', '/argentina/rosario/', '/australia/', '/australia/adelaide/', '/australia/brisbane/', '/australia/canberra/', '/australia/darwin/', '/australia/melbourne/', '/australia/perth/', '/australia/sydney/', '/austria/', '/austria/vienna/', '/bahrain/', '/belarus/', '/belarus/brest/', '/belarus/gomel/', '/belarus/grodno/', '/belarus/minsk/', '/belgium/', '/brazil/', '/brazil/belem/', '/brazil/belo-horizonte/', '/brazil/brasilia/', '/brazil/campinas/', '/brazil/curitiba/', '/brazil/fortaleza/', '/brazil/goiania/', '/brazil/guarulhos/', '/brazil/manaus/', '/brazil/porto-alegre/', '/brazil/recife/', '/brazil/rio-de-janeiro/', '/brazil/salvador/', '/brazil/sao-luis/', '/brazil/sao-paulo/', '/canada/', '/canada/calgary/', '/canada/edmonton/', '/canada/montreal/', '/canada/ottawa/', '/canada/quebec/', '/canada/toronto/', '/canada/vancouver/', '/canada/winnipeg/', '/chile/', '/chile/concepcion/', '/chile/santiago/', '/chile/valparaiso/', '/colombia/', '/colombia/barranquilla/', '/colombia/bogota/', '/colombia/cali/', '/colombia/medellin/', '/denmark/', '/dominican-republic/', '/dominican-republic/santo-domingo/', '/ecuador/', '/ecuador/guayaquil/', '/ecuador/quito/', '/egypt/', '/egypt/alexandria/', '/egypt/cairo/', '/egypt/giza/', '/france/', '/france/bordeaux/', '/france/lille/', '/france/lyon/', '/france/marseille/', '/france/montpellier/', '/france/nantes/', '/france/paris/', '/france/rennes/', '/france/strasbourg/', '/france/toulouse/', '/germany/', '/germany/berlin/', '/germany/bremen/', '/germany/cologne/', '/germany/dortmund/', '/germany/dresden/', '/germany/dusseldorf/', '/germany/essen/', '/germany/frankfurt/', '/germany/hamburg/', '/germany/leipzig/', '/germany/munich/', '/germany/stuttgart/', '/ghana/', '/ghana/accra/', '/ghana/kumasi/', '/greece/', '/greece/athens/', '/greece/thessaloniki/', '/guatemala/', '/guatemala/guatemala-city/', '/india/', '/india/ahmedabad/', '/india/amritsar/', '/india/bangalore/', '/india/bhopal/', '/india/chennai/', '/india/delhi/', '/india/hyderabad/', '/india/indore/', '/india/jaipur/', '/india/kanpur/', '/india/kolkata/', '/india/lucknow/', '/india/mumbai/', '/india/nagpur/', '/india/patna/', '/india/pune/', '/india/rajkot/', '/india/ranchi/', '/india/srinagar/', '/india/surat/', '/india/thane/', '/indonesia/', '/indonesia/bandung/', '/indonesia/bekasi/', '/indonesia/depok/', '/indonesia/jakarta/', '/indonesia/makassar/', '/indonesia/medan/', '/indonesia/palembang/', '/indonesia/pekanbaru/', '/indonesia/semarang/', '/indonesia/surabaya/', '/indonesia/tangerang/', '/ireland/', '/ireland/cork/', '/ireland/dublin/', '/ireland/galway/', '/israel/', '/israel/haifa/', '/israel/jerusalem/', '/israel/tel-aviv/', '/italy/', '/italy/bologna/', '/italy/genoa/', '/italy/milan/', '/italy/naples/', '/italy/palermo/', '/italy/rome/', '/italy/turin/', '/japan/', '/japan/chiba/', '/japan/fukuoka/', '/japan/hamamatsu/', '/japan/hiroshima/', '/japan/kawasaki/', '/japan/kitakyushu/', '/japan/kobe/', '/japan/kumamoto/', '/japan/kyoto/', '/japan/nagoya/', '/japan/niigata/', '/japan/okayama/', '/japan/okinawa/', '/japan/osaka/', '/japan/sagamihara/', '/japan/saitama/', '/japan/sapporo/', '/japan/sendai/', '/japan/takamatsu/', '/japan/tokyo/', '/japan/yokohama/', '/jordan/', '/jordan/amman/', '/kenya/', '/kenya/mombasa/', '/kenya/nairobi/', '/korea/', '/korea/ansan/', '/korea/bucheon/', '/korea/busan/', '/korea/changwon/', '/korea/daegu/', '/korea/daejeon/', '/korea/goyang/', '/korea/gwangju/', '/korea/incheon/', '/korea/seongnam/', '/korea/seoul/', '/korea/suwon/', '/korea/ulsan/', '/korea/yongin/', '/kuwait/', '/latvia/', '/latvia/riga/', '/lebanon/', '/malaysia/', '/malaysia/hulu-langat/', '/malaysia/ipoh/', '/malaysia/johor-bahru/', '/malaysia/kajang/', '/malaysia/klang/', '/malaysia/kuala-lumpur/', '/malaysia/petaling/', '/mexico/', '/mexico/acapulco/', '/mexico/aguascalientes/', '/mexico/chihuahua/', '/mexico/ciudad-juarez/', '/mexico/culiacan/', '/mexico/ecatepec-de-morelos/', '/mexico/guadalajara/', '/mexico/hermosillo/', '/mexico/leon/', '/mexico/merida/', '/mexico/mexicali/', '/mexico/mexico-city/', '/mexico/monterrey/', '/mexico/morelia/', '/mexico/naucalpan-de-juarez/', '/mexico/nezahualcoyotl/', '/mexico/puebla/', '/mexico/queretaro/', '/mexico/saltillo/', '/mexico/san-luis-potosi/', '/mexico/tijuana/', '/mexico/toluca/', '/mexico/zapopan/', '/netherlands/', '/netherlands/amsterdam/', '/netherlands/den-haag/', '/netherlands/rotterdam/', '/netherlands/utrecht/', '/new-zealand/', '/new-zealand/auckland/', '/nigeria/', '/nigeria/benin-city/', '/nigeria/ibadan/', '/nigeria/kaduna/', '/nigeria/kano/', '/nigeria/lagos/', '/nigeria/port-harcourt/', '/norway/', '/norway/bergen/', '/norway/oslo/', '/oman/', '/oman/muscat/', '/pakistan/', '/pakistan/faisalabad/', '/pakistan/karachi/', '/pakistan/lahore/', '/pakistan/multan/', '/pakistan/rawalpindi/', '/panama/', '/peru/', '/peru/lima/', '/philippines/', '/philippines/antipolo/', '/philippines/cagayan-de-oro/', '/philippines/calocan/', '/philippines/cebu-city/', '/philippines/davao-city/', '/philippines/makati/', '/philippines/manila/', '/philippines/pasig/', '/philippines/quezon-city/', '/philippines/taguig/', '/philippines/zamboanga-city/', '/poland/', '/poland/gdansk/', '/poland/krakow/', '/poland/lodz/', '/poland/poznan/', '/poland/warsaw/', '/poland/wroclaw/', '/portugal/', '/puerto-rico/', '/qatar/', '/russia/', '/russia/chelyabinsk/', '/russia/irkutsk/', '/russia/kazan/', '/russia/khabarovsk/', '/russia/krasnodar/', '/russia/krasnoyarsk/', '/russia/moscow/', '/russia/nizhny-novgorod/', '/russia/novosibirsk/', '/russia/omsk/', '/russia/perm/', '/russia/rostov-on-don/', '/russia/saint-petersburg/', '/russia/samara/', '/russia/ufa/', '/russia/vladivostok/', '/russia/volgograd/', '/russia/voronezh/', '/russia/yekaterinburg/', '/saudi-arabia/', '/saudi-arabia/ahsa/', '/saudi-arabia/dammam/', '/saudi-arabia/jeddah/', '/saudi-arabia/mecca/', '/saudi-arabia/medina/', '/saudi-arabia/riyadh/', '/singapore/', '/singapore/', '/south-africa/', '/south-africa/cape-town/', '/south-africa/durban/', '/south-africa/johannesburg/', '/south-africa/port-elizabeth/', '/south-africa/pretoria/', '/south-africa/soweto/', '/spain/', '/spain/barcelona/', '/spain/bilbao/', '/spain/las-palmas/', '/spain/madrid/', '/spain/malaga/', '/spain/murcia/', '/spain/palma/', '/spain/seville/', '/spain/valencia/', '/spain/zaragoza/', '/sweden/', '/sweden/gothenburg/', '/sweden/stockholm/', '/switzerland/', '/switzerland/geneva/', '/switzerland/lausanne/', '/switzerland/zurich/', '/thailand/', '/thailand/bangkok/', '/turkey/', '/turkey/adana/', '/turkey/ankara/', '/turkey/antalya/', '/turkey/bursa/', '/turkey/diyarbakır/', '/turkey/eskisehir/', '/turkey/gaziantep/', '/turkey/istanbul/', '/turkey/izmir/', '/turkey/kayseri/', '/turkey/konya/', '/turkey/mersin/', '/ukraine/', '/ukraine/dnipropetrovsk/', '/ukraine/donetsk/', '/ukraine/kharkiv/', '/ukraine/kyiv/', '/ukraine/lviv/', '/ukraine/odesa/', '/ukraine/zaporozhye/', '/united-arab-emirates/', '/united-arab-emirates/abu-dhabi/', '/united-arab-emirates/dubai/', '/united-arab-emirates/sharjah/', '/united-kingdom/', '/united-kingdom/belfast/', '/united-kingdom/birmingham/', '/united-kingdom/blackpool/', '/united-kingdom/bournemouth/', '/united-kingdom/brighton/', '/united-kingdom/bristol/', '/united-kingdom/cardiff/', '/united-kingdom/coventry/', '/united-kingdom/derby/', '/united-kingdom/edinburgh/', '/united-kingdom/glasgow/', '/united-kingdom/hull/', '/united-kingdom/leeds/', '/united-kingdom/leicester/', '/united-kingdom/liverpool/', '/united-kingdom/london/', '/united-kingdom/manchester/', '/united-kingdom/middlesbrough/', '/united-kingdom/newcastle/', '/united-kingdom/nottingham/', '/united-kingdom/plymouth/', '/united-kingdom/portsmouth/', '/united-kingdom/preston/', '/united-kingdom/sheffield/', '/united-kingdom/stoke-on-trent/', '/united-kingdom/swansea/', '/united-states/', '/united-states/albuquerque/', '/united-states/atlanta/', '/united-states/austin/', '/united-states/baltimore/', '/united-states/baton-rouge/', '/united-states/birmingham/', '/united-states/boston/', '/united-states/charlotte/', '/united-states/chicago/', '/united-states/cincinnati/', '/united-states/cleveland/', '/united-states/colorado-springs/', '/united-states/columbus/', '/united-states/dallas-ft-worth/', '/united-states/denver/', '/united-states/detroit/', '/united-states/el-paso/', '/united-states/fresno/', '/united-states/greensboro/', '/united-states/harrisburg/', '/united-states/honolulu/', '/united-states/houston/', '/united-states/indianapolis/', '/united-states/jackson/', '/united-states/jacksonville/', '/united-states/kansas-city/', '/united-states/las-vegas/', '/united-states/long-beach/', '/united-states/los-angeles/', '/united-states/louisville/', '/united-states/memphis/', '/united-states/mesa/', '/united-states/miami/', '/united-states/milwaukee/', '/united-states/minneapolis/', '/united-states/nashville/', '/united-states/new-haven/', '/united-states/new-orleans/', '/united-states/new-york/', '/united-states/norfolk/', '/united-states/oklahoma-city/', '/united-states/omaha/', '/united-states/orlando/', '/united-states/philadelphia/', '/united-states/phoenix/', '/united-states/pittsburgh/', '/united-states/portland/', '/united-states/providence/', '/united-states/raleigh/', '/united-states/richmond/', '/united-states/sacramento/', '/united-states/salt-lake-city/', '/united-states/san-antonio/', '/united-states/san-diego/', '/united-states/san-francisco/', '/united-states/san-jose/', '/united-states/seattle/', '/united-states/st-louis/', '/united-states/tallahassee/', '/united-states/tampa/', '/united-states/tucson/', '/united-states/virginia-beach/', '/united-states/washington/', '/venezuela/', '/venezuela/barcelona/', '/venezuela/barquisimeto/', '/venezuela/caracas/', '/venezuela/ciudad-guayana/', '/venezuela/maracaibo/', '/venezuela/maracay/', '/venezuela/maturin/', '/venezuela/turmero/', '/venezuela/valencia/', '/vietnam/', '/vietnam/can-tho/', '/vietnam/da-nang/', '/vietnam/hai-phong/', '/vietnam/hanoi/', '/vietnam/ho-chi-minh-city/']
    if os.path.exists('trending_hashtags.json') and datetime.strptime(json.loads(open('trending_hashtags.json','r').read())['load_date'], '%Y-%m-%d %H:%M:%S') > (datetime.now() - timedelta(hours=3)):
        all_hashtags = json.loads(open('trending_hashtags.json','r').read())['hashtags']
    else:
        all_hashtags = {}
        def _get(url):
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                for trend_card in soup.find('div', attrs={'class': 'trend-card'}):
                    for trends in trend_card.find_all('li'):
                        try:
                            hashtag = trends.find('a').get_text(strip=True)
                            count = int(trends.find('span').get_text(strip=True).replace('K', '000'))
                            if '#' in hashtag:
                                all_hashtags[hashtag] = count
                        except Exception as ex:
                            continue
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_url = {executor.submit(_get, f"https://trends24.in{url}"): url for url in trends_in}
            for future in as_completed(future_to_url):
                pass
        all_hashtags = dict(sorted(all_hashtags.items(), key=lambda item: item[1]))
    try:
        top_hashtag = list(all_hashtags.keys())[-1]
        del all_hashtags[top_hashtag]
        json.dump({"load_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S%z'), "hashtags": all_hashtags}, open('trending_hashtags.json', 'w'))
    except IndexError:
        top_hashtag = None
    return top_hashtag

def get_tweets_for_time_window(start, end, limit, labels, run_type):
    scraper = TwitterScraper(
        since_date=start,
        until_date=end,
        limit=limit,
        labels=labels,
        run_type=run_type
    )
    return scraper.search()

def divide_time_into_windows(start_date, end_date, number_of_windows):
    total_duration = end_date - start_date
    if number_of_windows > 1:
        window_duration = total_duration / number_of_windows
    else:
        window_duration = total_duration
    windows = []
    current_start = start_date
    for _ in range(number_of_windows):
        current_end = current_start + window_duration
        if current_end > end_date:
            current_end = end_date
        windows.append((current_start, current_end))
        current_start = current_end
    return windows

def fetch_tweets_in_parallel_v1(since_date, until_date, labels, max_items=100, max_workers=2, run_type='production'):
    all_tweets = []
    time_windows = divide_time_into_windows(since_date, until_date, max_workers)
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_time_window = {executor.submit(get_tweets_for_time_window, start, end, int(max_items/max_workers), labels, run_type): (start, end) for start, end in time_windows}
            for future in as_completed(future_to_time_window):
                time_window = future_to_time_window[future]
                try:
                    tweets_data = future.result()
                    all_tweets.extend(tweets_data)
                    # print(f"Successfully fetched data for {time_window}: {tweets_data}")
                except Exception as exc:
                    print(f"Failed to fetch data for {time_window}: {exc}")
        
        bt.logging.info(f"Total tweets fetched: {len(all_tweets)}")
    except Exception as e:
        bt.logging.error(f"parallel search failed: {e}")
    return all_tweets

def fetch_tweets_in_parallel_v2(since_date, until_date, labels, max_items=10000, max_workers=2, run_type='production'):
    all_tweets = []
    hashtag_since_date= ""
    try:
        search_params = []
        if not labels:
            for _ in range(max_workers):
                # Fetch top hashtag
                while True:
                    top_hashtag = get_top_trends24_hashtag()
                    if not top_hashtag:
                        break

                    # Get last scraped since_date
                    # hashtag_since_date = get_last_tweettime_for_hashtag(top_hashtag) # need to change the db connection
                    break
                
                if not hashtag_since_date:
                    hashtag_since_date = datetime.now(timezone.utc) - timedelta(days=2)
                
                if not top_hashtag:
                    search_params.append((hashtag_since_date, None, [], max_items))
                else:
                    search_params.append((hashtag_since_date, None, [top_hashtag], max_items))

            for _ in range(max_workers-len(search_params)):
                search_params.append((since_date, until_date, labels, max_items))
        else:
            time_windows = divide_time_into_windows(since_date, until_date, max_workers)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            if not labels:
                future_to_time_window = {executor.submit(get_tweets_for_time_window, param[0], param[1], param[3], param[2], run_type): (param[0], param[1]) for param in search_params}
            else:
                future_to_time_window = {executor.submit(get_tweets_for_time_window, start, end, max_items, labels, run_type): (start, end) for start, end in time_windows}
            for future in as_completed(future_to_time_window):
                time_window = future_to_time_window[future]
                try:
                    tweets_data = future.result()
                    all_tweets.extend(tweets_data)
                except Exception as exc:
                    # traceback.print_exc()
                    print(f"Failed to fetch data for {time_window}: {exc}")
        
        bt.logging.info(f"Total tweets fetched: {len(all_tweets)}")
    except Exception as e:
        # traceback.print_exc()
        bt.logging.error(f"parallel search failed: {e}")
    return all_tweets

if __name__ == '__main__':
    start = datetime.now()
    print("Time Taken to scrape: ", (datetime.now() - start).seconds/60)