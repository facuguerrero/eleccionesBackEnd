import random
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
        # followers_to_update = random.sample(self.updating_followers, 1500)
        # for follower in followers_to_update.keys():
        #     self.updating_followers.pop(follower)
        # if len(self.updating_followers) < UMBRAL:
        #     self.logger.info('Adding new followers to update their tweets.')
        #     #TODO descomentar --  Como hacerlo en un thread aparte para no retrasar esta descarga?
        #     cls.add_followers_to_be_updated()
        # # return followers_to_update
        return {"278744817": "Fri May 21 02:43:14 +0000 2010"}

    def add_followers_to_be_updated(self, new_followers):
        ConcurrencyUtils().acquire_lock('followers_for_update_tweets')
        # TODO new_followers tiene que agarrarse de la base de datos aca.
        self.updating_followers.update(new_followers)
        ConcurrencyUtils().release_lock('followers_for_update_tweets')
