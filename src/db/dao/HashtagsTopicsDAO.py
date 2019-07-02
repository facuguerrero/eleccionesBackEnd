from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class HashtagsTopicsDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(HashtagsTopicsDAO, self).__init__(Mongo().get().db.hashtags_topics)
        self.logger = Logger(self.__class__.__name__)

    def get_required_hashtags(self, required_hashtags):
        """ Retrieve all hashtags which are included in required_hashtags. """
        documents = self.get_all({'hashtag': {'$in': required_hashtags}})
        hashtags = {}
        for hashtag in documents:
            hashtags[hashtag['hashtag']] = hashtag['topics']
        return hashtags
