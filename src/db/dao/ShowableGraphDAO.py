from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NoCooccurrenceGraphError import NoCooccurrenceGraphError
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class ShowableGraphDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(ShowableGraphDAO, self).__init__(Mongo().get().db.showable_graphs)
        self.logger = Logger(self.__class__.__name__)

    def store(self, graphs, start_date, end_date):
        """ Store main graph and all topic graphs into collection. """
        documents = [{'topic_id': key,
                      'graph': graph,
                      'start_date': start_date,
                      'end_date': end_date}
                     for key, graph in graphs.items()]
        self.collection.insert_many(documents)

    def find(self, topic_id, start_date, end_date):
        """ Retrieve graph in given window with given id. """
        query = {'start_date': start_date, 'end_date': end_date, 'topic_id': topic_id}
        document = self.get_first(query)
        return document['graph']

    def find_all(self, start_date, end_date):
        """ Retrieve graph in given window with given id. """
        query = {'start_date': start_date, 'end_date': end_date}
        documents = self.get_all(query, {'graph': 1, 'topic_id': 1})
        return documents
