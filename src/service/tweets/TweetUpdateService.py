import time
from twython import Twython, TwythonRateLimitError, TwythonError
from datetime import datetime

from src.exception import CredentialsAlreadyInUseError

from src.service.credentials.CredentialService import CredentialService
from src.util.logging.Logger import Logger

MAX_TWEETS = 200
PRIVATE_USER_ERROR_CODE = 401


class TweetUpdateService:

    @classmethod
    def update_tweets(cls):
        """ Update tweet of some candidates' followers. """
        cls.get_logger().info('Starting follower updating process.')
        try:
            credential = CredentialService().get_credential_for_service(cls.__name__)
        except CredentialsAlreadyInUseError as caiue:
            cls.get_logger().error(caiue.message)
            cls.get_logger().warning('Tweets updating process skipped.')
            return
        #
        twitter = cls.twitter(credential)
        tweet = cls.download_follower_tweets(twitter, "1121513395243581447", True)

    @classmethod
    def download_follower_tweets(cls, twitter, follower_id, is_first_request, max_id=None):
        tweets = []
        try:
            if is_first_request:
                tweets = twitter.get_user_timeline(user_id=follower_id, include_rts=True, count=MAX_TWEETS)
            else:
                tweets = twitter.get_user_timeline(user_id=follower_id, include_rts=True, max_id=max_id,
                                                   count=MAX_TWEETS)
        except TwythonError as error:
            if error.error_code == PRIVATE_USER_ERROR_CODE:
                cls.get_logger().warning(f'User with id {follower_id} is private.')
                # TODO marcar en la base que el usuario es privado
            else:
                cls.get_logger().error(
                    f'An unknown error occurred while trying to download tweets from: {follower_id}.')
                cls.get_logger().error(error)

        except TwythonRateLimitError:
            cls.get_logger().warning('Tweets download limit reached. Waiting.')
            # TODO ver que hacer cuando se alcanza el limite
        return tweets

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
        return Logger('FollowerUpdateService')
