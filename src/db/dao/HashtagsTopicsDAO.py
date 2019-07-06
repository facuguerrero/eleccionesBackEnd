from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class HashtagsTopicsDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(HashtagsTopicsDAO, self).__init__(Mongo().get().db.hashtags_topics)
        self.logger = Logger(self.__class__.__name__)

    def store(self, hashtags_topics, start_date, end_date):
        """ Store main graph and all topic graphs into collection. """
        documents = [{'hashtag': hashtag,
                      'topics': list(topics),
                      'start_date': start_date,
                      'end_date': end_date}
                     for hashtag, topics in hashtags_topics.items()]
        self.collection.insert_many(documents)

    def get_required_hashtags(self, all_topics, all_hashtags):
        """ Retrieve all topics by hashtags. """
        hashtags_topics = self.get_all()
        position_vectors = []

        for hashtag_topics in hashtags_topics:
            hashtag_index = all_hashtags.index(hashtag_topics['hashtag'])
            for topic in hashtag_topics['topics']:
                position_vectors.append([hashtag_index, int(topic), 1])
        return position_vectors
