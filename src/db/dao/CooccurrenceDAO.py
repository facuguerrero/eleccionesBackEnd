from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.DateUtils import DateUtils
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CooccurrenceDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(CooccurrenceDAO, self).__init__(Mongo().get().db.cooccurrence)
        self.logger = Logger(self.__class__.__name__)

    def store(self, tweet, pair):
        # Generate document
        document = {'user_id': str(tweet['user_id']),
                    'created_at': tweet['created_at'],
                    'pair': pair}
        # Store document
        self.collection.insert_one(document)

    def exists_in_tweet_day(self, tweet, pair):
        """ Verifies if a given hashtag pair was used by a certain user in a given window of time. """
        start_date, end_date = DateUtils.first_and_last_seconds(tweet['created_at'])
        query = {'user_id': str(tweet['user_id']),
                 'pair': pair,
                 'created_at': {'$gt': start_date, '$lt': end_date}}
        return self.get_first(query) is not None

    def find_in_window(self, start_date, end_date):
        """ Retrieve all pairs of hashtags in time window. """
        return self.get_all({'created_at': {'$gt': start_date, '$lt': end_date}}, {'pair': 1, '_id': 0})

    def distinct_users(self, hashtag, start_date, end_date):
        """ Returns a list of all the different users that used the given hashtag in the given window. """
        query = {'pair': hashtag, 'created_at': {'$gt': start_date, '$lt': end_date}}
        return self.collection.distinct('user_id', query)

    def create_indexes(self):
        self.logger.info('Creating [user_id, pair, created_at] index for collection cooccurrence_graphs.')
        self.collection.create_index(['user_id', 'pair', 'created_at'])
