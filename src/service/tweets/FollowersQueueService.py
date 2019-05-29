import random
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class FollowersQueueService(metaclass=Singleton):

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.updating_followers = {}
        ConcurrencyUtils().create_lock('followers_for_update_tweets')

    def get_followers_to_update(self):
        self.logger.info('Getting followers to update their tweets.')

        max_users_per_window = ConfigurationManager().get_int('max_users_per_window')
        followers_to_update = {}

        # Acquire lock for get the followers
        ConcurrencyUtils().acquire_lock('followers_for_update_tweets')
        random_followers_keys = random.sample(self.updating_followers.keys(), max_users_per_window)
        # Remove selected followers
        for follower in random_followers_keys:
            followers_to_update[follower] = self.updating_followers.pop(follower)

        if len(self.updating_followers) <= max_users_per_window:
            # Retrieve more candidates from db
            self.add_followers_to_be_updated()
        ConcurrencyUtils().release_lock('followers_for_update_tweets')
        return followers_to_update

    def add_followers_to_be_updated(self):
        # TODO Analizar productor y consumidor en python.
        self.logger.info('Adding new followers to update their tweets.')
        new_followers = RawFollowerDAO().get_public_and_not_updated_users()
        self.updating_followers.update(new_followers)
