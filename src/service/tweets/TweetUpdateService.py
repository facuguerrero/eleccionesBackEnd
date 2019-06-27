import datetime
import time

import pytz
from twython import TwythonRateLimitError, TwythonError
from urllib3.exceptions import ProtocolError

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.exception import CredentialsAlreadyInUseError
from src.exception.BlockedCredentialError import BlockedCredentialError
from src.exception.DuplicatedTweetError import DuplicatedTweetError
from src.exception.NoMoreFollowersToUpdateTweetsError import NoMoreFollowersToUpdateTweetsError
from src.exception.NonExistentRawFollowerError import NonExistentRawFollowerError
from src.model.followers.RawFollower import RawFollower
from src.service.credentials.CredentialService import CredentialService
from src.service.hashtags.HashtagCooccurrenceService import HashtagCooccurrenceService
from src.service.hashtags.HashtagOriginService import HashtagOriginService
from src.service.hashtags.UserHashtagService import UserHashtagService
from src.service.tweets.FollowersQueueService import FollowersQueueService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger
from src.util.slack.SlackHelper import SlackHelper
from src.util.twitter.TwitterUtils import TwitterUtils


class TweetUpdateService:

    def __init__(self):
        self.contiguous_private_users = 0
        self.contiguous_limit_error = 0
        self.continue_downloading = False
        self.start_time = None

    def download_tweets_with_credential(self, credential):
        """ Update followers' tweets with an specific Twitter Api Credential. """
        self.get_logger().info(f'Starting follower updating with credential {credential.id}.')
        # Create Twython instance for credential
        twitter = TwitterUtils.twitter_with_app_auth(credential)
        try:
            self.tweets_update_process(twitter, credential.id)
        except BlockedCredentialError:
            self.get_logger().error(f'credential with id {credential.id} seems to be blocked')
        except Exception as e:
            self.get_logger().error(e)
            self.send_stopped_tread_notification(credential.id)

    def tweets_update_process(self, twitter, credential_id):
        """ Method to catch any exception """
        followers = self.get_followers_to_update()

        # While there are followers to update
        self.start_time = datetime.datetime.today()
        while followers:
            for follower, last_update in followers.items():
                self.continue_downloading = False
                min_tweet_date = last_update.astimezone(pytz.timezone('America/Argentina/Buenos_Aires'))
                follower_download_tweets = self.download_tweets_and_validate(twitter, follower, min_tweet_date, True)

                # While retrieve new tweets
                while self.continue_downloading:
                    max_id = follower_download_tweets[len(follower_download_tweets) - 1]['id'] - 1
                    follower_download_tweets += self.download_tweets_and_validate(twitter, follower, min_tweet_date,
                                                                                  False, max_id)
                self.store_tweets_and_update_follower(follower_download_tweets, follower, min_tweet_date)
                # cls.get_logger().warning(f'Follower updated {follower}.')
            followers = self.get_followers_to_update()
        self.send_stopped_tread_notification(credential_id)

    def download_tweets_and_validate(self, twitter, follower, min_tweet_date, is_first_request, max_id=None):
        """ Download tweets and check if continue downloading. """
        download_tweets = self.do_download_tweets_request(twitter, follower, is_first_request, max_id)
        if len(download_tweets) != 0:
            last_tweet = download_tweets[len(download_tweets) - 1]
            self.continue_downloading = self.check_if_continue_downloading(last_tweet, min_tweet_date)
        else:
            self.continue_downloading = False
        return download_tweets

    def do_download_tweets_request(self, twitter, follower, is_first_request, max_id=None):
        """
        @is_first_request is True, max_id parameter is not included in the request.
        @max_id is to get the maximum quantity of tweets per request.
        """
        tweets = []
        try:
            # Sleep to avoid (104, 'Connection reset by peer')
            # https://stackoverflow.com/questions/383738/104-connection-reset-by-peer-socket-error-or-when-does-closing-a-socket-resu
            time.sleep(0.01)
            max_tweets_request_parameter = ConfigurationManager().get_int('max_tweets_parameter')
            if is_first_request:
                tweets = twitter.get_user_timeline(user_id=follower, include_rts=True, tweet_mode='extended',
                                                   count=max_tweets_request_parameter)
            else:
                tweets = twitter.get_user_timeline(user_id=follower, include_rts=True, tweet_mode='extended',
                                                   count=max_tweets_request_parameter, max_id=max_id)
            self.contiguous_private_users = 0
            self.contiguous_limit_error = 0
        except TwythonRateLimitError:
            self.handle_twython_rate_limit_error()
        except TwythonError as error:
            self.handle_twython_generic_error(error, follower)
        except (ProtocolError, ConnectionResetError):
            # ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))
            self.get_logger().error('Connection error. Try again later.')
        return tweets

    def handle_twython_rate_limit_error(self):
        if self.contiguous_limit_error > 3:
            SlackHelper().post_message_to_channel(
                "Por prevención se freno el update de una credencial.", "#errors")
            raise BlockedCredentialError()
        self.contiguous_limit_error += 1
        duration = (datetime.datetime.today() - self.start_time).seconds
        self.get_logger().warning(f'Tweets download limit reached. Waiting. Execution time: {str(duration)}')

        # By default, wait 900 segs
        time_default = ConfigurationManager().get_int('tweets_download_sleep_seconds')
        time_to_sleep = (time_default - duration) if (time_default >= duration) else time_default
        time.sleep(time_to_sleep)

        self.get_logger().info(f'Waiting done. Resuming follower updating. Wait '
                               f'for: {(datetime.datetime.today() - self.start_time).seconds}')
        self.start_time = datetime.datetime.today()

    def handle_twython_generic_error(self, error, follower):
        if (error.error_code == ConfigurationManager().get_int('private_user_error_code') or
                error.error_code == ConfigurationManager().get_int('not_found_user_error_code')):
            if self.contiguous_private_users >= 100:
                SlackHelper().post_message_to_channel(
                    "Muchos usuarios privados.", "#errors")
                raise BlockedCredentialError()
            self.contiguous_private_users += 1
            self.update_follower_as_private(follower)
        elif not error or not error.error_code or error.error_code < 199 or error.error_code >= 500:
            # Twitter API error
            # More information: https://developer.twitter.com/en/docs/basics/response-codes.html
            self.get_logger().error('Twitter API error. Try again later.')
        else:
            self.get_logger().error(
                f'An unknown error occurred while trying to download tweets from: {follower}.')
            self.get_logger().error(error)

    def store_tweets_and_update_follower(self, follower_download_tweets, follower, min_tweet_date):
        if len(follower_download_tweets) != 0:
            last_tweet_date = self.get_formatted_date(follower_download_tweets[0]['created_at'])
            self.update_complete_follower(follower, follower_download_tweets[0], last_tweet_date)
            self.store_new_tweets(follower_download_tweets, min_tweet_date)
        else:
            self.update_follower_with_no_tweets(follower)

    # CLASS METHODS

    @classmethod
    def get_followers_to_update(cls):
        """ Get the followers to be updated from FollowersQueueService. """
        try:
            return FollowersQueueService().get_followers_to_update()
        except NoMoreFollowersToUpdateTweetsError:
            return None

    @classmethod
    def send_stopped_tread_notification(cls, credential_id):
        cls.get_logger().warning(f'Stoping follower updating proccess with {credential_id}.')
        SlackHelper().post_message_to_channel(
            "Un thread del servicio TweetUpdateService dejo de funcionar.", "#errors")
        CredentialService().unlock_credential(credential_id, cls.__name__)

    @classmethod
    def update_follower_as_private(cls, follower):
        """ When an error occurs, follower is tagged as private. """
        try:
            # Retrieve the follower from DB
            today = datetime.datetime.today()
            updated_raw_follower = RawFollower(**{
                'id': follower,
                'downloaded_on': today,
                'is_private': True
            })
            RawFollowerDAO().update_follower_data(updated_raw_follower)
            # cls.get_logger().info(f'{follower} is tagged as private.')
        except NonExistentRawFollowerError as error:
            cls.get_logger().error(f'{follower} can not be tagged as private because does not exists.')
            cls.get_logger().error(error)

    @classmethod
    def update_complete_follower(cls, follower, tweet, last_tweet_date):
        """ Update follower's last download date. """
        try:
            today = datetime.datetime.today()
            if 'user' in tweet:
                user_information = tweet['user']
                updated_raw_follower = RawFollower(**{
                    'id': follower,
                    'downloaded_on': today,
                    'last_tweet_date': last_tweet_date,
                    'is_private': False,
                    'has_tweets': True,
                    'location': user_information['location'],
                    'followers_count': user_information['followers_count'],
                    'friends_count': user_information['friends_count'],
                    'listed_count': user_information['listed_count'],
                    'favourites_count': user_information['favourites_count'],
                    'statuses_count': user_information['statuses_count']
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
                RawFollowerDAO().update_follower_downloaded_on(follower)
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
                try:
                    tweet_copy = tweet.copy()
                    tweet_copy["_id"] = tweet.pop('id_str', None)
                    tweet_copy.pop('id', None)
                    tweet_copy["text"] = tweet.pop('full_text', None)
                    tweet_copy['created_at'] = tweet_date
                    tweet_copy['user_id'] = tweet.pop('user')['id_str']
                    tweet_copy['in_user_hashtag_collection'] = True
                    RawTweetDAO().insert_tweet(tweet_copy)
                    HashtagOriginService().process_tweet(tweet_copy)
                    HashtagCooccurrenceService().process_tweet(tweet_copy)
                    UserHashtagService().insert_hashtags_of_one_tweet(tweet_copy)
                    updated_tweets += 1
                except DuplicatedTweetError:
                    #cls.get_logger().info(
                    #    f'{updated_tweets} tweets of {tweet["user"]["id"]} are updated. Actual date: {tweet_date}')
                    return
            else:
                #cls.get_logger().info(
                #   f'{updated_tweets} tweets of {tweet["user"]["id"]} are updated. Actual date: {tweet_date}')
                return

    @classmethod
    def check_if_continue_downloading(cls, last_tweet, min_tweet_date):
        """" Return True if the oldest download's tweet is greater than min_date required. """
        last_tweet_date = cls.get_formatted_date(last_tweet['created_at'])
        if last_tweet_date is None or min_tweet_date is None: return False
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
