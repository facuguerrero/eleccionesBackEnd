from pymongo.errors import DuplicateKeyError

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.DuplicatedTweetError import DuplicatedTweetError
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class RawTweetDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(RawTweetDAO, self).__init__(Mongo().get().db.raw_tweets)
        # self.__dict__.update(**kwargs)
        self.logger = Logger(self.__class__.__name__)

    def insert_tweet(self, raw_tweet):
        """ Adds RawTweet to data base using upsert to update 'follows' list."""
        try:
            self.insert(raw_tweet)
        except DuplicateKeyError:
            # self.logger.warning(f'Trying to insert a duplicated tweet {raw_tweet["user_id"]}.')
            raise DuplicatedTweetError

    def cooccurrence_checked(self, tweet):
        """ Mark tweet as checked for hashtag cooccurrence. """
        self.update_first({'_id': tweet['_id']}, {'cooccurrence_checked': True})

    def hashtag_origin_checked(self, tweet):
        """ Mark tweet as checked for hashtag origin. """
        self.update_first({'_id': tweet['_id']}, {'hashtag_origin_checked': True})

    def get_rt_to_candidates_cursor(self, candidates):
        """ Get tweets which are rt to one candidate.
            If one tweet has retweeted_status field
            then this tweet is rt without comments or extra text.
        """
        return self.find(
            {"$and": [
                {'retweeted_status': {'$exists': True}},
                {'retweeted_status.user.screen_name': {'$in': candidates}}
            ]}
        )


#   def create_indexes(self):
# self.logger.info('Creating user_id index for collection raw_tweets.')
# Mongo().get().db.tweets.create_index('user_id')
# self.logger.info('Creating retweeted_status index for collection raw_tweets.')
# Mongo().get().db.tweets.create_index('retweeted_status')
# self.logger.info('Creating created_at index for collection raw_tweets.')
# Mongo().get().db.tweets.create_index('created_at')
