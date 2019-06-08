import datetime
import time

import pytz
from twython import TwythonRateLimitError, TwythonError

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.exception import CredentialsAlreadyInUseError
from src.exception.DuplicatedTweetError import DuplicatedTweetError
from src.exception.NoMoreFollowersToUpdateTweetsError import NoMoreFollowersToUpdateTweetsError
from src.exception.NonExistentRawFollowerError import NonExistentRawFollowerError
from src.model.followers.RawFollower import RawFollower
from src.service.credentials.CredentialService import CredentialService
from src.service.tweets.FollowersQueueService import FollowersQueueService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger
from src.util.slack.SlackHelper import SlackHelper
from src.util.twitter.TwitterUtils import TwitterUtils


class TweetUpdateService:

    @classmethod
    def update_tweets(cls):
        """ Update tweet of some candidates' followers. """
        cls.get_logger().info('Starting follower updating process.')
        try:
            credentials = CredentialService().get_all_credentials_for_service(cls.__name__)
        except CredentialsAlreadyInUseError as caiue:
            cls.get_logger().error(caiue.message)
            cls.get_logger().warning('Tweets updating process skipped.')
            return
        # Run tweet update process
        AsyncThreadPoolExecutor().run(cls.download_tweets_with_credential, credentials)
        # cls.download_tweets_with_credential(credentials[2])
        cls.get_logger().info('Stoped tweet updating')
        SlackHelper().post_message_to_channel(
            "El servicio TweetUpdateService dejo de funcionar. Se frenaron todos los threads.")

    @classmethod
    def download_tweets_with_credential(cls, credential):
        """ Update followers' tweets with an specific Twitter Api Credential. """
        cls.get_logger().info(f'Starting follower updating with credential {credential.id}.')
        # Create Twython instance for credential
        twitter = TwitterUtils.twitter_with_app_auth(credential)
        try:
            cls.tweets_update_process(twitter, credential.id)
        except Exception as e:
            cls.send_stopped_tread_notification(credential.id)

    @classmethod
    def tweets_update_process(cls, twitter, credential_id):
        """ Method to catch any exception """
        # While there are followers to update
        followers = cls.get_followers_to_update()
        start_time = time.time()
        min_tweet_date = datetime.datetime(2018, 12, 31, 23, 59, 59).astimezone(
            pytz.timezone('America/Argentina/Buenos_Aires'))
        while followers:
            for follower, last_update in followers.items():
                # TODO descomentar luego de la primera pasada
                # min_tweet_date = last_update.astimezone(pytz.timezone('America/Argentina/Buenos_Aires'))
                # TODO cuando termine la primera ronda poner el parametro trim_user = true para
                # que no devuelva todo el usuario. tambien sacar el update_follower
                result = cls.download_tweets_and_validate(twitter, follower,
                                                          min_tweet_date, start_time, True)
                continue_downloading = result[0]
                follower_download_tweets = result[1]
                start_time = result[2]
                while continue_downloading:
                    max_id = follower_download_tweets[len(follower_download_tweets) - 1]['id'] - 1
                    result = cls.download_tweets_and_validate(twitter, follower,
                                                              min_tweet_date, start_time, False, max_id)
                    continue_downloading = result[0]
                    follower_download_tweets = result[1]
                    start_time = result[2]
                if len(follower_download_tweets) != 0:
                    cls.update_complete_follower(follower, follower_download_tweets[0])
                    cls.store_new_tweets(follower_download_tweets, min_tweet_date)
                else:
                    cls.update_follower_with_no_tweets(follower)
                # cls.get_logger().warning(f'Follower updated {follower}.')
            followers = cls.get_followers_to_update()
        cls.send_stopped_tread_notification(credential_id)

    @classmethod
    def send_stopped_tread_notification(cls, credential_id):
        cls.get_logger().warning(f'Stoping follower updating proccess with {credential_id}.')
        SlackHelper().post_message_to_channel(
            "Un thread del servicio TweetUpdateService dejo de funcionar.")
        CredentialService().unlock_credential(credential_id, cls.__name__)

    @classmethod
    def get_followers_to_update(cls):
        """ Get the followers to be updated from FollowersQueueService. """
        try:
            return FollowersQueueService().get_followers_to_update()
        except NoMoreFollowersToUpdateTweetsError:
            return None

    @classmethod
    def download_tweets_and_validate(cls, twitter, follower, min_tweet_date,
                                     start_time, is_first_request, max_id=None):
        """ Download tweets. If there are not new results, return false to end the download. """
        result = cls.do_download_tweets_request(twitter, follower, start_time, is_first_request, max_id)
        follower_download_tweets = []
        download_tweets = result[0]
        time_to_return = result[1]
        if len(download_tweets) != 0:
            last_tweet = download_tweets[len(download_tweets) - 1]
            follower_download_tweets += download_tweets
            return (
                cls.check_if_continue_downloading(last_tweet, min_tweet_date), follower_download_tweets, time_to_return)
        return (False, follower_download_tweets, time_to_return)

    @classmethod
    def do_download_tweets_request(cls, twitter, follower, start_time, is_first_request, max_id=None):
        """
        @is_first_request is True, max_id parameter is not included in the request.
        @max_id is to get the maximum quantity of tweets per request.
        """
        tweets = []
        time_to_return = start_time
        try:
            max_tweets_request_parameter = ConfigurationManager().get_int('max_tweets_parameter')
            if is_first_request:
                tweets = twitter.get_user_timeline(user_id=follower, include_rts=True, tweet_mode='extended',
                                                   count=max_tweets_request_parameter)
            else:
                tweets = twitter.get_user_timeline(user_id=follower, include_rts=True, tweet_mode='extended',
                                                   count=max_tweets_request_parameter, max_id=max_id)
        except TwythonRateLimitError:
            duration = int(time.time() - start_time) + 1
            cls.get_logger().warning(f'Tweets download limit reached. Waiting. Execution time: {str(duration)}')
            # By default, wait 900 segs
            time_default = ConfigurationManager().get_int('tweets_download_sleep_seconds')
            time_to_sleep = time_default
            # If 900 >= execution time
            if time_default >= duration:
                time_to_sleep = time_default - duration
            time.sleep(time_to_sleep)
            time_to_return = time.time()
            duration = time_to_return - start_time
            cls.get_logger().info(f'Waiting done. Resuming follower updating. Wait for: {duration}')
        except TwythonError as error:
            if (error.error_code == ConfigurationManager().get_int('private_user_error_code') or
                    error.error_code == ConfigurationManager().get_int('not_found_user_error_code')):
                cls.update_follower_as_private(follower)
            elif error.error_code <= 199 or error.error_code >= 500:
                # Twitter API error
                # More information: https://developer.twitter.com/en/docs/basics/response-codes.html
                #  ConnectionResetError(104, 'Connection reset by peer')
                cls.get_logger().error('Twitter API error. Try again later.')

            else:
                cls.get_logger().error(
                    f'An unknown error occurred while trying to download tweets from: {follower}.')
                cls.get_logger().error(error)
        return (tweets, time_to_return)

    @classmethod
    def update_follower_as_private(cls, follower):
        """ When an error occurs, follower is tagged as private. """
        try:
            # Retrieve the follower from DB
            raw_follower = RawFollowerDAO().get(follower)
            RawFollowerDAO().tag_as_private(raw_follower)
            # cls.get_logger().info(f'{follower} is tagged as private.')
        except NonExistentRawFollowerError as error:
            cls.get_logger().error(f'{follower} can not be tagged as private because does not exists.')
            cls.get_logger().error(error)

    @classmethod
    def update_complete_follower(cls, follower, tweet):
        """ Update follower's last download date. """
        try:
            today = datetime.datetime.today()
            if 'user' in tweet:
                user_information = tweet['user']
                updated_raw_follower = RawFollower(**{
                    'id': follower,
                    'downloaded_on': today,
                    'location': user_information['location'],
                    'followers_count': user_information['followers_count'],
                    'friends_count': user_information['friends_count'],
                    'listed_count': user_information['listed_count'],
                    'favourites_count': user_information['favourites_count'],
                    'statuses_count': user_information['statuses_count'],
                    'has_tweets': True
                })
                RawFollowerDAO().update_follower_data(updated_raw_follower)
                # cls.get_logger().info(f'{follower} is completely updated.')
            else:
                cls.update_follower_with_no_tweets(follower)
        except NonExistentRawFollowerError:
            cls.get_logger().error(f'Follower {follower} does not exists')

    @classmethod
    def update_follower_with_no_tweets(cls, follower):
        """ Update follower's last download date. """
        try:
            raw_follower = RawFollowerDAO().get(follower)
            if not raw_follower.is_private:
                today = datetime.datetime.today()
                updated_raw_follower = RawFollower(**{
                    'id': follower,
                    'downloaded_on': today,
                    'has_tweets': False
                })
                RawFollowerDAO().update_follower_data(updated_raw_follower)
                # cls.get_logger().info(f'{follower} is updated with 0 tweets.')
        except NonExistentRawFollowerError:
            cls.get_logger().error(f'Follower {follower} does not exists')

    @classmethod
    def store_new_tweets(cls, follower_download_tweets, min_tweet_date):
        """ Store new follower's tweet since last update. """
        updated_tweets = 0
        for tweet in follower_download_tweets:
            tweet_date = cls.get_formatted_date(tweet['created_at'])
            if tweet_date >= min_tweet_date:
                # Clean tweet's information
                try:
                    tweet_copy = tweet.copy()
                    tweet_copy["_id"] = tweet['id_str']
                    tweet_copy.pop('id')
                    tweet_copy.pop('id_str')
                    tweet_copy["text"] = tweet['full_text']
                    tweet_copy.pop('full_text')
                    tweet_copy['created_at'] = tweet_date
                    tweet_copy['user_id'] = tweet['user']['id_str']
                    tweet_copy.pop('user')
                    RawTweetDAO().insert_tweet(tweet_copy)
                    updated_tweets += 1
                except KeyError:
                    RawTweetDAO().insert_tweet(tweet_copy)
                    cls.get_logger().error(f'Key error in tweet with id {tweet_copy["_id"]}')
                except DuplicatedTweetError:
                    return
            else:
                # cls.get_logger().info(
                #    f'{updated_tweets} tweets of {tweet["user"]["id"]} are updated. Actual date: {tweet_date}')
                return

    @classmethod
    def check_if_continue_downloading(cls, last_tweet, min_tweet_date):
        """" Return True if the oldest download's tweet is greater than min_date required. """
        last_tweet_date = cls.get_formatted_date(last_tweet['created_at'])
        return min_tweet_date < last_tweet_date

    @classmethod
    def get_formatted_date(cls, date):
        """ Format date. """
        try:
            return datetime.datetime.strptime(date, '%a %b %d %H:%M:%S %z %Y').astimezone(
                pytz.timezone('America/Argentina/Buenos_Aires'))
        except ValueError as error:
            cls.get_logger().error(f'Invalid date format {date}.')
            cls.get_logger().error(f'{error}')

    @classmethod
    def get_logger(cls):
        return Logger('TweetUpdateService')
