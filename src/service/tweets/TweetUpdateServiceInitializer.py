from src.exception.CredentialsAlreadyInUseError import CredentialsAlreadyInUseError
from src.service.credentials.CredentialService import CredentialService
from src.service.tweets.TweetUpdateService import TweetUpdateService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton
from src.util.slack.SlackHelper import SlackHelper


class TweetUpdateServiceInitializer(metaclass=Singleton):

    @classmethod
    def initialize_tweet_update_service(cls):
        """ Update tweet of some candidates' followers. """
        cls.get_logger().info('Starting follower updating process.')
        try:
            credentials = CredentialService().get_all_credentials_for_service(cls.__name__)
        except CredentialsAlreadyInUseError as caiue:
            cls.get_logger().error(caiue.message)
            cls.get_logger().warning('Tweets updating process skipped.')
            return
        # Run tweet update process
        AsyncThreadPoolExecutor().run(cls.initialize_with_credential, credentials)
        # self.download_tweets_with_credential(credentials[0])
        cls.get_logger().info('Stopped tweet updating')
        SlackHelper().post_message_to_channel(
            "El servicio TweetUpdateService dejo de funcionar. Se frenaron todos los threads.", "#errors")

    @classmethod
    def initialize_with_credential(cls, credential):
        TweetUpdateService().download_tweets_with_credential(credential)


    @classmethod
    def get_logger(cls):
        return Logger('TweetUpdateService')
