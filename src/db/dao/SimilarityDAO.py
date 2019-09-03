from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class SimilarityDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(SimilarityDAO, self).__init__(Mongo().get().db.users_similarities)
        self.logger = Logger(self.__class__.__name__)

    def insert_similarities(self, similarity_object):
        """ Put new similarity data. """
        self.logger.error(f'{similarity_object.timestamp}')
        self.logger.error(f'{similarity_object.similarities}')
        update_dict = {'_id': similarity_object.timestamp, 'similarities': similarity_object.similarities}
        self.insert(update_dict)
