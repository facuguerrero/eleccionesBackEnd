from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.exception.CredentialsAlreadyInUseError import CredentialsAlreadyInUseError
from src.model.Credential import Credential
from src.service.credentials.CredentialService import CredentialService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.concurrency.MultiThreadQueue import MultiThreadQueue
from src.util.logging.Logger import Logger


class UserNetworkService:

    __pool = MultiThreadQueue()

    @classmethod
    def do_retrieval(cls):
        """ Use all possible credentials to download users' friends. """
        # Get credentials for service
        cls.get_logger().info('Starting user friends retrieval.')
        try:
            credentials = CredentialService().get_all_credentials_for_service(cls.__name__)
        except CredentialsAlreadyInUseError as caiue:
            cls.get_logger().error(caiue.message)
            return
        # Fill pool with users
        cls.populate_user_pool()
        # Run follower update process
        AsyncThreadPoolExecutor().run(cls.retrieve_with_credential, credentials)
        cls.get_logger().info('Finished user friends retrieval.')

    @classmethod
    def retrieve_with_credential(cls, credential: Credential):
        """ Download users' friends with given credential. """

    @classmethod
    def populate_user_pool(cls):
        """ Retrieve users from database for friend downloading. """
        # TODO: Create query to get active non-retrieved users
        pool_users = RawFollowerDAO().get_all({})
        # Store users in service's queue to poll
        cls.__pool.put_many(pool_users)

    @classmethod
    def user_from_pool(cls) -> str:
        """ Get a user id from the pool to retrieve data. """

    @classmethod
    def user_friends(cls, user_id: str) -> set[str]:
        """ Retrieve user friend set. """

    @classmethod
    def active_friends(cls, friends: set[str], active_users: set[str]) -> set[str]:
        """ Intersect friends set with active users set. """

    @classmethod
    def store_active_friends_set(cls, user_id: str, active_friends: set[str]):
        """ Store set of active friends for given user in database. """

    @classmethod
    def get_logger(cls):
        return Logger(cls.__name__)
