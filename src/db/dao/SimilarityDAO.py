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
        date = similarity_object.timestamp
        update_dict = {'_id': str(date.year) + str(date.month) + str(date.day),
                       'similarities': similarity_object.similarities,
                       'similarities_without_random': similarity_object.similarities_wor,
                       'date': date}
        self.insert(update_dict)

    def delete_and_insert_similarities(self, similarity_object):
        date = similarity_object.timestamp
        name = str(date.year) + str(date.month) + str(date.day)
        self.delete_first({'_id': name})
        self.insert_similarities(similarity_object)
