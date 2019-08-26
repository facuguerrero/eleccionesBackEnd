from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class HashtagUsageDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(HashtagUsageDAO, self).__init__(Mongo().get().db.hashtag_usage)
        self.logger = Logger(self.__class__.__name__)

    def store(self, hashtag_name, start_date, end_date, date_axis, count_axis, parties_vectors):
        """ Stores plottable data for a hashtag in a given time window. """
        document = {'hashtag_name': hashtag_name,
                    'start_date': start_date,
                    'end_date': end_date,
                    'date_axis': date_axis,
                    'count_axis': count_axis,
                    'parties_vectors': parties_vectors}
        self.insert(document)

    def find(self, hashtag_name, start_date, end_date):
        """ Retrieves plottable data for a hashtag in a given time window. """
        document = self.get_first({'hashtag_name': hashtag_name, 'start_date': start_date, 'end_date': end_date})
        if not document:
            raise NoDocumentsFoundError(hashtag_name, None)
        return {'date_axis': document['date_axis'],
                'count_axis': document['count_axis'],
                'parties_vectors': document['parties_vectors']}
