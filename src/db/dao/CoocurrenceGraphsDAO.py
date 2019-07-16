from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CoocurrenceGraphsDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(CoocurrenceGraphsDAO, self).__init__(Mongo().get().db.cooccurrence_graphs)
        self.logger = Logger(self.__class__.__name__)

    def get_all_sorted_topics(self):
        graphs = self.get_all({}, {'topic_id': 1})
        topic_ids = set()
        for graph in graphs:
            topic_ids.add(graph['topic_id'])
        topics_list = list(topic_ids)
        return sorted(topics_list)
