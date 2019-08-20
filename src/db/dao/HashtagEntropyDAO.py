from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class HashtagEntropyDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(HashtagEntropyDAO, self).__init__(Mongo().get().db.hashtag_entropy)
        self.logger = Logger(self.__class__.__name__)

    def store_vector(self, hashtag, vector):
        """ Creates a document relating the given hashtag and its entropy vector and stores it in the database. """
        self.insert({'_id': hashtag, 'vector': vector})

    def find(self, hashtag):
        """ Get the entropy vector for the given hashtag. None if the given hashtag was never analyzed. """
        return self.get_first({'_id': hashtag}, {'vector': 1, '_id': 0})
