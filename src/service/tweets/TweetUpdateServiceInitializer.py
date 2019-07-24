from threading import Thread

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
        cls.run_process_with_credentials(credentials)

    @classmethod
    def restart_credential(cls, credential_id):
        cls.get_logger().error('After waiting, restart credential.')
        try:
            # Use TweetUpdateService class name due to the credential is unlocked in that class.
            credential = CredentialService().get_credential_with_id_for_service(credential_id,
                                                                                TweetUpdateService.__class__.__name__)
        except NoAvailableCredentialsError:
            cls.get_logger().error('Can not restart credential.')
            return
        cls.get_logger().error(f'Restarting credential with id: {credential_id}.')
        cls.run_process_with_credentials([credential])

    @classmethod
    def run_process_with_credentials(cls, credentials):
        # Run tweet update process
        AsyncThreadPoolExecutor().run(cls.initialize_with_credential, credentials)
        # cls.initialize_with_credential(credentials[0])

        cls.get_logger().info('Stopped tweet updating')
        SlackHelper().post_message_to_channel(
            "El servicio TweetUpdateService dejo de funcionar. Se frenaron todos los threads.", "#errors")

    @classmethod
    def initialize_with_credential(cls, credential):
        TweetUpdateService().download_tweets_with_credential(credential)

    @classmethod
    def get_logger(cls):
        return Logger('TweetUpdateService')
