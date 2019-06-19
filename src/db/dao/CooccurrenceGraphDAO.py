from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NoCooccurrenceGraphError import NoCooccurrenceGraphError
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CooccurrenceGraphDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(CooccurrenceGraphDAO, self).__init__(Mongo().get().db.cooccurrence_graphs)
        self.logger = Logger(self.__class__.__name__)

    def store(self, graph, start_date, end_date):
        """ Store graph into collection. """
        document = {'graph': graph,
                    'start_date': start_date,
                    'end_date': end_date}
        self.insert(document)

    def find(self, start_date, end_date):
        """ Retrieve graph in given window. """
        document = self.get_first({'start_date': start_date, 'end_date': end_date})
        if not document:
            raise NoCooccurrenceGraphError(start_date, end_date)
        return document['graph']
