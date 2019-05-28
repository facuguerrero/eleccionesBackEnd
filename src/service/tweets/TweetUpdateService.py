import pytz
import datetime
from twython import Twython, TwythonRateLimitError, TwythonError

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.exception import CredentialsAlreadyInUseError
from src.exception.NonExistentRawFollowerError import NonExistentRawFollowerError
from src.model.followers.RawFollower import RawFollower

from src.model.tweets.RawTweet import RawTweet

from src.service.credentials.CredentialService import CredentialService
from src.service.tweets.FollowersQueueService import FollowersQueueService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor

from src.util.logging.Logger import Logger

#TODO pasar esto al properties.cfg
MAX_TWEETS = 200
PRIVATE_USER_ERROR_CODE = 401


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
        # TODO cambuar esto antes de subirlo al servidor
        #AsyncThreadPoolExecutor().run(cls.download_tweets_with_credential, credentials)
        cls.download_tweets_with_credential(credentials[0])
        cls.get_logger().info('Stoped tweet updating')

    @classmethod
    def download_tweets_with_credential(cls, credential):
        """ Update followers' tweets with an specific Twitter Api Credential. """
        cls.get_logger().info(f'Starting follower updating with credential {credential.id}.')
        # Create Twython instance for credential
        twitter = cls.twitter(credential)
        # While there are followers to update
        followers = cls.get_followers_to_update()
        while followers:
            for follower, last_update in followers.items():
                follower_download_tweets = []
                min_tweet_date = cls.get_formatted_date(last_update)
                continue_downloading = cls.download_tweets_and_validate(twitter, follower, follower_download_tweets,
                                                                        min_tweet_date, True)
                while continue_downloading:
                    max_id = follower_download_tweets[len(follower_download_tweets) - 1]['id'] - 1
                    continue_downloading = cls.download_tweets_and_validate(twitter, follower, follower_download_tweets,
                                                                            min_tweet_date, False, max_id)
                cls.store_new_tweets(follower, follower_download_tweets, min_tweet_date)
                if len(follower_download_tweets) != 0:
                    cls.update_follower(follower)
            followers = cls.get_followers_to_update()
        cls.get_logger().warning(f'Stoping follower updating proccess with {credential}.')
        CredentialService().unlock_credential(credential, cls.__name__)

    @classmethod
    def get_followers_to_update(cls):
        """ Get the followers to be updated from FollowersQueueService. """
        return FollowersQueueService().get_followers_to_update()

    @classmethod
    def download_tweets_and_validate(cls, twitter, follower, follower_download_tweets, min_tweet_date,
                                     is_first_request, max_id=None):
        """ Download tweets. If there are not new results, return false to end the download. """
        download_tweets = cls.do_download_tweets_request(twitter, follower, is_first_request, max_id)
        if len(download_tweets) != 0:
            last_tweet = download_tweets[len(download_tweets) - 1]
            follower_download_tweets += download_tweets
            return cls.check_if_continue_downloading(last_tweet, min_tweet_date)
        return False

    @classmethod
    def do_download_tweets_request(cls, twitter, follower, is_first_request, max_id=None):
        """
        @is_first_request is True, max_id parameter is not included in the request.
        @max_id is to get the maximum quantity of tweets per request.
        """
        tweets = []
        try:
            if is_first_request:
                tweets = twitter.get_user_timeline(user_id=follower, include_rts=True, count=MAX_TWEETS)
            else:
                tweets = twitter.get_user_timeline(user_id=follower, include_rts=True, count=MAX_TWEETS,
                                                   max_id=max_id)
        except TwythonError as error:
            if error.error_code == PRIVATE_USER_ERROR_CODE:
                cls.get_logger().warning(f'User with id {follower} is private.')
                cls.update_follower_as_private(follower)
            else:
                cls.get_logger().error(
                    f'An unknown error occurred while trying to download tweets from: {follower}.')
                cls.get_logger().error(error)
        except TwythonRateLimitError:
            cls.get_logger().warning('Tweets download limit reached. Waiting.')
            # TODO ver que hacer cuando se alcanza el limite
        return tweets

    @classmethod
    def update_follower_as_private(cls, follower):
        """ When an error occurs, follower is tagged as private. """
        try:
            # Retrieve the follower from DB
            raw_follower = RawFollowerDAO().get(follower)
            RawFollowerDAO().tag_as_private(raw_follower)
            cls.get_logger().info(f'{follower} is tagged as private.')
        except NonExistentRawFollowerError as error:
            cls.get_logger().error(f'{follower} can not be tagged as private.')

    @classmethod
    def update_follower(cls, follower):
        """ Update follower's last download date. """
        today = datetime.datetime.today().astimezone(pytz.timezone('America/Argentina/Buenos_Aires'))
        # Retrieve the follower from DB
        raw_follower = RawFollowerDAO().get(follower)
        updated_raw_follower = RawFollower(**{'id': follower,
                                              'follows': raw_follower.follows,
                                              'downloaded_on': today})
        RawFollowerDAO().put(updated_raw_follower)
        cls.get_logger().info(f'{follower} is updated.')

    @classmethod
    def store_new_tweets(cls, follower, follower_download_tweets, min_tweet_date):
        """ Store new follower's tweet since last update. """
        for tweet in follower_download_tweets:
            tweet_date = tweet['created_at_datetime'] = cls.get_formatted_date(tweet['created_at'])
            if tweet_date >= min_tweet_date:
                raw_tweet = RawTweet(**{'id': tweet['id'],
                                        'created_at': tweet_date,
                                        'text': tweet['text'],
                                        'user_id': tweet['user']['id']})
                RawTweetDAO().put(raw_tweet)
        cls.get_logger().info(f'Tweets of {follower} are updated.')

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
    def twitter(cls, credential):
        """ Create Twython instance depending on credential data. """
        if credential.access_token is None:
            twitter = Twython(app_key=credential.consumer_key, app_secret=credential.consumer_secret)
        elif credential.consumer_key is None:
            twitter = Twython(oauth_token=credential.access_token, oauth_token_secret=credential.access_secret)
        else:
            twitter = Twython(app_key=credential.consumer_key, app_secret=credential.consumer_secret,
                              oauth_token=credential.access_token, oauth_token_secret=credential.access_secret)
        return twitter

    @classmethod
    def get_logger(cls):
        return Logger('TweetUpdateService')
