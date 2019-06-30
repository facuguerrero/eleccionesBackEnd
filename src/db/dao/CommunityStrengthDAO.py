from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NoCooccurrenceGraphError import NoCooccurrenceGraphError
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CommunityStrengthDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(CommunityStrengthDAO, self).__init__(Mongo().get().db.community_strength)
        self.logger = Logger(self.__class__.__name__)

    def store(self, community_strength, start_date, end_date):
        """ Store community strength data into collection. """
        document = {'community_strength': community_strength,
                    'start_date': start_date,
                    'end_date': end_date}
        self.insert(document)

    def find(self, start_date, end_date):
        """ Retrieve community strength data in given window. """
        document = self.get_first({'start_date': start_date, 'end_date': end_date})
        if not document:
            raise NoCooccurrenceGraphError(start_date, end_date)
        return document['community_strength']
