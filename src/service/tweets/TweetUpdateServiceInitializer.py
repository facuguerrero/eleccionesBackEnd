from threading import Thread

from src.exception.BlockedCredentialError import BlockedCredentialError
from src.exception.CredentialsAlreadyInUseError import CredentialsAlreadyInUseError
from src.exception.NoAvailableCredentialsError import NoAvailableCredentialsError
from src.service.credentials.CredentialService import CredentialService
from src.service.tweets.TweetUpdateService import TweetUpdateService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton
from src.util.slack.SlackHelper import SlackHelper


class TweetUpdateServiceInitializer(metaclass=Singleton):

    @classmethod
    def initialize_tweet_update_service(cls):
        thread = Thread(target=cls.initialize_tweet_update_service_with_credentials)
        thread.start()

    @classmethod
    def initialize_tweet_update_service_with_credentials(cls):
        """ Update tweet of some candidates' followers. """
        cls.get_logger().info('Starting follower updating process.')
        try:
            credentials = CredentialService().get_all_credentials_for_service(cls.__name__)
        except CredentialsAlreadyInUseError as caiue:
            cls.get_logger().error(caiue.message)
            cls.get_logger().warning('Tweets updating process skipped.')
            return
        # Run tweet update process
        try:
            AsyncThreadPoolExecutor().run(cls.initialize_with_credential, credentials)
        except BlockedCredentialError as error:
            cls.get_logger().info(f'After waiting, restart credential {error.credential}.')
            cls.restart_credential(error.credential)
        # self.download_tweets_with_credential(credentials[0])
        cls.get_logger().info('Stopped tweet updating')
        SlackHelper().post_message_to_channel(
            "El servicio TweetUpdateService dejo de funcionar. Se frenaron todos los threads.", "#errors")

    @classmethod
    def restart_credential(cls, credential_id):
        try:
            credential = CredentialService().get_credential_with_id_for_service(credential_id, cls.__name__)
        except NoAvailableCredentialsError:
            cls.get_logger().error('Can not restart credential.')
            return
        AsyncThreadPoolExecutor().run(cls.initialize_with_credential, [credential])
        # self.download_tweets_with_credential(credentials[0])
        cls.get_logger().info('Stopped tweet updating with restarted credential')
        SlackHelper().post_message_to_channel(
            "Se freno la credencial que fue re-starteada.", "#errors")

    @classmethod
    def initialize_with_credential(cls, credential):
        TweetUpdateService().download_tweets_with_credential(credential)

    @classmethod
    def get_logger(cls):
        return Logger('TweetUpdateService')
