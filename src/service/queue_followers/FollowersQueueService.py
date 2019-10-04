import random
from datetime import datetime

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.exception.NoMoreFollowersToUpdateTweetsError import NoMoreFollowersToUpdateTweetsError
from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton
from src.util.slack.SlackHelper import SlackHelper


class FollowersQueueService(metaclass=Singleton):

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.updating_followers = {}
        self.priority_updating_followers = {}
        self.processing_followers = set()
        ConcurrencyUtils().create_lock('followers_for_update_tweets')

    def get_followers_to_update(self, followers_to_delete):
        # Acquire lock for get the followers
        ConcurrencyUtils().acquire_lock('followers_for_update_tweets')
        self.logger.info(f'Getting followers to update their tweets. Queue\'s size: {len(self.updating_followers)} ')

        followers_to_update = self.try_to_get_priority_followers()
        if len(followers_to_update) == 0:
            followers_to_update = self.get_followers_with_tweets_to_update()

        self.processing_followers.update(set(followers_to_update.keys()))
        self.processing_followers = self.processing_followers.difference(followers_to_delete)
        ConcurrencyUtils().release_lock('followers_for_update_tweets')

        return followers_to_update

    def try_to_get_priority_followers(self):
        # If we have recent downloaded followers
        users_to_update = {}
        if len(self.priority_updating_followers) != 0:
            self.logger.warning(f'Getting {len(self.priority_updating_followers)} recent downloaded followers.')
            users_to_update = self.priority_updating_followers.copy()
            self.priority_updating_followers = {}
        return users_to_update

    def get_followers_with_tweets_to_update(self):
        """ Get followers with tweets to update. """
        max_users_per_window = ConfigurationManager().get_int('max_users_per_window')

        self.check_if_have_followers(max_users_per_window)

        # Get the min follower's quantity between length and max_users
        min_length = min(max_users_per_window, len(self.updating_followers.keys()))
        random_followers_keys = random.sample(self.updating_followers.keys(), min_length)

        # Remove selected followers
        followers_to_update = {}
        for follower in random_followers_keys:
            followers_to_update[follower] = self.updating_followers.pop(follower)
        return followers_to_update

    def check_if_have_followers(self, max_users_per_window):

        if len(self.updating_followers) <= 2 * max_users_per_window:
            # Retrieve more candidates from db
            self.add_followers_to_be_updated()

        if len(self.updating_followers) == 0:
            SlackHelper().post_message_to_channel(
                "No se obtuvieron seguidores de la base de datos.")
            self.logger.error('There are not followers to update their tweets.')
            raise NoMoreFollowersToUpdateTweetsError()

    def add_followers_to_be_updated(self, timedelta=36):
        self.logger.info(
            f'Adding new followers to update their tweets. Actual size: {str(len(self.updating_followers))}')
        followers = RawFollowerDAO().get_random_followers_sample(list(self.processing_followers), timedelta)
        new_followers = self.add_followers(followers)
        if len(new_followers) == 0:
            # If there are no new results
            self.logger.error('Can\'t retrieve followers to update their tweets. ')
            raise NoMoreFollowersToUpdateTweetsError()
        self.updating_followers.update(new_followers)

    def add_not_updated_followers_2(self):
        self.logger.info(
            f'Adding not updated followers.')
        self.add_followers_to_be_updated(55)

    def add_not_updated_followers_1(self):
        self.logger.info(
            f'Adding not updated followers.')
        self.add_followers_to_be_updated(60)

    def add_last_downloaded_followers(self):
        self.logger.info('Adding last downloaded followers')
        users_to_be_updated = RawFollowerDAO().get_all({
            '$and': [
                {'has_tweets': {'$exists': False}},
                {'is_private': {'$ne': True}}
            ]})
        followers = self.add_followers(users_to_be_updated)
        self.priority_updating_followers.update(followers)
        self.logger.info('Finishing insertion of last downloaded followers')

    def add_followers(self, downloaded):
        followers = {}
        for follower in downloaded:
            date = datetime(2019, 1, 1)
            if 'last_tweet_date' in follower and follower['last_tweet_date'] is not None:
                date = follower['last_tweet_date']
            if date is None:
                self.logger.warning(f"None type for: {follower['_id']}")
            followers[follower['_id']] = date
        self.logger.info(f"Added {len(followers)} to queue.")
        return followers
