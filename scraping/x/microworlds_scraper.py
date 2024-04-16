# import sys
# sys.path.append("/mnt/d/Google Drive/Temp/Desktop/Freelance/Clients/Potiential Clients/Varun S/data-universe")
from datetime import datetime

import asyncio
import threading
import traceback
import bittensor as bt
from typing import List
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunner
from scraping.x.model import XContent
from scraping.x import utils
from scraping.twitter_scraper import TwitterScraper, fetch_tweets_in_parallel_v1, fetch_tweets_in_parallel_v2
import datetime as dt
import nest_asyncio
asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
nest_asyncio.apply()

class MicroworldsTwitterScraper(Scraper):
    """
    Scrapes tweets using the Microworlds Twitter Scraper: https://console.apify.com/actors/heLL6fUofdPgRXZie.
    """

    ACTOR_ID = "heLL6fUofdPgRXZie"

    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {
        "maxRequestRetries": 5,
        "searchMode": "live",
    }

    # As of 2/5/24 this actor only takes 256 MB in the default config so we can run a full batch without hitting shared actor memory limits.
    concurrent_validates_semaphore = threading.BoundedSemaphore(20)

    def __init__(self, runner: ActorRunner = ActorRunner()):
        self.runner = runner

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URI."""

        async def validate_entity(entity) -> ValidationResult:
            if not utils.is_valid_twitter_url(entity.uri):
                return ValidationResult(
                    is_valid=False,
                    reason="Invalid URI.",
                    content_size_bytes_validated=entity.content_size_bytes,
                )

            attempt = 0
            max_attempts = 2
            while attempt < max_attempts:
                # Increment attempt.
                attempt += 1

                # On attempt 1 we fetch the exact number of tweets. On retry we fetch more in case they are in replies.
                tweet_count = 1 if attempt == 1 else 5


                # Retrieve the tweets from Apify.
                dataset: List[dict] = None
                try:
                    dataset: List[dict] = TwitterScraper_V1(
                        uri=entity.uri
                    ).tweet()
                except (
                    Exception
                ) as e:  # Catch all exceptions here to ensure we do not exit validation early.
                    if attempt != max_attempts:
                        # Retrying.
                        continue
                    else:
                        bt.logging.error(
                            f"Failed to run actor: {traceback.format_exc()}."
                        )
                        # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                        # one caused by malicious input. In my own testing I was able to make the Actor timeout by
                        # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                        # pass malicious input for chunks they don't have.
                        return ValidationResult(
                            is_valid=False,
                            reason="Failed to run Actor. This can happen if the URI is invalid, or APIfy is having an issue.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )

                # Parse the response
                tweets = self._best_effort_parse_dataset_v2(dataset, 'tweet')

                actual_tweet = None
                for tweet in tweets:
                    if tweet.url == entity.uri:
                        actual_tweet = tweet
                        break
                if actual_tweet is None:
                    # Only append a failed result if on final attempt.
                    if attempt == max_attempts:
                        return ValidationResult(
                            is_valid=False,
                            reason="Tweet not found or is invalid.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                else:
                    require_obfuscation = (
                        actual_tweet.timestamp
                        >= constants.REDUCED_CONTENT_DATETIME_GRANULARITY_THRESHOLD
                    )
                    return utils.validate_tweet_content(
                        actual_tweet=actual_tweet,
                        entity=entity,
                        require_obfuscated_content_date=require_obfuscation,
                    )

        if not entities:
            return []

        # Since we are using the threading.semaphore we need to use it in a context outside of asyncio.
        bt.logging.trace("Acquiring semaphore for concurrent microworlds validations.")
        with MicroworldsTwitterScraper.concurrent_validates_semaphore:
            bt.logging.trace(
                "Acquired semaphore for concurrent microworlds validations."
            )
            results = await asyncio.gather(
                *[validate_entity(entity) for entity in entities]
            )

        return results


    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        # Construct the query string.
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"
        query = f"since:{scrape_config.date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)} until:{scrape_config.date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)}"
        if scrape_config.labels:
            label_query = " OR ".join([label.value for label in scrape_config.labels])
            query += f" ({label_query})"
        else:
            # HACK: The search query doesn't work if only a time range is provided.
            # If no label is specified, just search for "e", the most common letter in the English alphabet.
            # I attempted using "#" instead, but that still returned empty results ¯\_(ツ)_/¯
            query += " e"

        # Construct the input to the runner.
        max_items = scrape_config.entity_limit or 150
        
        if scrape_config.labels:
            labels = [label.value for label in scrape_config.labels]
        else:
            labels = []

        bt.logging.trace(f"Performing Twitter scrape for search terms: {query}.")

        # Run the Actor and retrieve the scraped data.
        dataset: List[dict] = None
        try:
            # dataset: List[dict] = search_scrape(query, max_items)
            # dataset: List[dict] = TwitterScraper_V1(
            #     since_date=scrape_config.date_range.start.astimezone(tz=dt.timezone.utc), 
            #     until_date=scrape_config.date_range.end.astimezone(tz=dt.timezone.utc), 
            #     limit=max_items, 
            #     labels=scrape_config.labels).search()
             dataset: List[dict] = fetch_tweets_in_parallel_v2(
                since_date=scrape_config.date_range.start.astimezone(tz=dt.timezone.utc),
                until_date=scrape_config.date_range.end.astimezone(tz=dt.timezone.utc),
                max_items=max_items,
                max_workers=7,
                labels=labels
            )
        except Exception:
            bt.logging.error(
                f"Failed to scrape tweets using search terms {query}: {traceback.format_exc()}."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        x_contents = self._best_effort_parse_dataset_v2(dataset, 'search')
        return x_contents


    def _best_effort_parse_dataset_v2(self, dataset: List[dict], type: str) -> List[XContent]:
        """Performs a best effort parsing of dataset into List[XContent]

        Any errors are logged and ignored."""
        if not dataset:
            return []

        results: List[XContent] = []
        failed = []
        for data in dataset:
            if type == 'search':
                try:
                    if 'tweet' in data['content']['itemContent']['tweet_results']['result']:
                        tweet_data = data['content']['itemContent']['tweet_results']['result']['tweet']['legacy']
                        user_data = data['content']['itemContent']['tweet_results']['result']['tweet']['core']['user_results']['result']['legacy']
                    else:
                        tweet_data = data['content']['itemContent']['tweet_results']['result']['legacy']
                        user_data = data['content']['itemContent']['tweet_results']['result']['core']['user_results']['result']['legacy']
                except Exception as e:
                    tweet_data = {}
                    user_data = {}  
                    # failed.append(data)
                    # bt.logging.error('result' in data['content']['itemContent']['tweet_results']['result'], list(data['content']['itemContent']['tweet_results']['result'].keys()))
                    # bt.logging.error('result' in data['content']['itemContent']['tweet_results']['result']['core']['user_results']['result'], list(data['content']['itemContent']['tweet_results']['result']['core']['user_results']['result'].keys()))
                    bt.logging.error('Error while parsing search tweet data: ', e)
            elif type == 'tweet':
                try:
                    tweet_data = data['data']['tweetResult'][0]['result']['legacy']
                    user_data = data['data']['tweetResult'][0]['result']['core']['user_results']['result']['legacy']
                except Exception as e:
                    tweet_data = {}
                    user_data = {}
                    bt.logging.error('error in tweet_data and user_data', e)
            try:
                # Check that we have the required fields.
                if (
                    ("full_text" not in tweet_data)
                    or ('id_str' not in tweet_data  and 'screen_name' not in user_data)
                    or "created_at" not in tweet_data
                ):
                    continue

                # Truncated_full_text is only populated if "full_text" is truncated.
                text = (
                    tweet_data["full_text"]
                )

                image_urls = tweet_data.get('entities', 'N/A').get('media', 'N/A')
                if image_urls != 'N/A':
                    image_urls = [media['media_url_https'] for media in image_urls if media['type'] == 'photo']
                else:
                    image_urls = 'N/A'
                    
                
                results.append(
                   {"id":tweet_data['id_str'],"tweet_content":text,"username":str(user_data["screen_name"]),"user_id":str(tweet_data['user_id_str']), "created_at":str(tweet_data["created_at"]),"url": str(f'https://twitter.com/{user_data["screen_name"]}/status/{tweet_data["id_str"]}'), "favourite_count":tweet_data['favorite_count'], "scraped_at":datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "image_urls":str(image_urls) }
                )
            except Exception:
                bt.logging.warning(
                    f"Parsing failed: {traceback.format_exc()}."
                )
        # json.dump(failed, open('failed.json', 'w'))
        return results

