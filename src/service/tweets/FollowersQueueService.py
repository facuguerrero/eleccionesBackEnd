import random
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton

UMBRAL = 3000


class FollowersQueueService(metaclass=Singleton):

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.updating_followers = {}
        ConcurrencyUtils().create_lock('followers_for_update_tweets')
        self.logger.info('Init FollowerQueueService.')

    def get_followers_to_update(self):
        self.logger.info('Getting followers to update their tweets.')
        self.add_followers_to_be_updated()

        # Acquire lock for get the followers
        ConcurrencyUtils().acquire_lock('followers_for_update_tweets')
        random_followers_keys = random.sample(self.updating_followers.keys(), 1500)
        # Remove selected followers
        followers_to_update = {}
        for follower in random_followers_keys:
            followers_to_update[follower] = self.updating_followers.pop(follower)
        # TODO este unlock queda aca y volvemos a tomar el lock en add followers o liberamos luego de actualizar
        ConcurrencyUtils().release_lock('followers_for_update_tweets')

        if len(self.updating_followers) < UMBRAL:
            # Retrieve more candidates from db
            self.logger.info('Adding new followers to update their tweets.')
            self.add_followers_to_be_updated()
            
        # return {"278744817": "Fri May 21 02:43:14 +0000 2010"}
        return followers_to_update

    def add_followers_to_be_updated(self):
        ConcurrencyUtils().acquire_lock('followers_for_update_tweets')
        new_followers = RawFollowerDAO().get_public_and_not_updated_users()
        self.updating_followers.update(new_followers)
        ConcurrencyUtils().release_lock('followers_for_update_tweets')
