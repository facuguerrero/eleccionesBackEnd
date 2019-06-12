from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
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

    def find_in_window(self, start_date, end_date):
        """ Retrieve all pairs of hashtags in time window. """
        return self.get_all({'created_at': {'$gt': start_date, '$lt': end_date}}, {'pair': 1, '_id': 0})
