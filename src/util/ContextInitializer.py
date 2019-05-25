from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.CandidatesFollowersDAO import CandidatesFollowersDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.service.candidates.CandidateService import CandidateService
from src.service.credentials.CredentialService import CredentialService
from src.service.followers.FollowerUpdateService import FollowerUpdateService
from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger
from src.util.scheduling.Scheduler import Scheduler
from src.service.tweets.FollowersQueueService import FollowersQueueService
from src.service.tweets.TweetUpdateService import TweetUpdateService


class ContextInitializer:

    LOGGER = Logger('ContextInitializer')

    @classmethod
    def initialize_context(cls):
        """ Create instances of all environment services in a Spring-like fashion."""
        cls.LOGGER.info('Instantiating context services and components.')
        ConfigurationManager()
        ConcurrencyUtils()
        Scheduler()
        CandidateDAO()
        RawFollowerDAO()
        CandidatesFollowersDAO()
        CredentialService()
        CandidateService()
        FollowerUpdateService()
        TweetUpdateService()
        FollowersQueueService()
