from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class TopicUsageDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(TopicUsageDAO, self).__init__(Mongo().get().db.topic_usage)
        self.logger = Logger(self.__class__.__name__)

    def store(self, topic_id, start_date, end_date, date_axis, count_axis):
        """ Stores plottable data for a topic in a given time window. """
        document = {'topic_id': topic_id,
                    'start_date': start_date,
                    'end_date': end_date,
                    'date_axis': date_axis,
                    'count_axis': count_axis}
        self.insert(document)

    def find(self, topic_id, start_date, end_date):
        """ Retrieves plottable data for a topic in a given time window. """
        document = self.get_first({'topic_id': topic_id, 'start_date': start_date, 'end_date': end_date})
        if not document:
            raise NoDocumentsFoundError(topic_id, None)
        return {'date_axis': document['date_axis'], 'count_axis': document['count_axis']}