import datetime

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class UserHashtagDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(UserHashtagDAO, self).__init__(Mongo().get().db.user_hashtag)
        self.logger = Logger(self.__class__.__name__)

    def get_hashtags_yesterday_aggregated_by_user(self):
        # TODO preguntar a que hora correrlo.
        date = datetime.datetime.today()
        documents = self.aggregate([
            {'$match': {'timestamp': {'$lt': date}}},
            {'$group': {
                '_id': '$user',
                'hashtags_array': {'$push': '$hashtag'}
            }
            }
        ])
        hashtags_by_user = {}
        for document in documents:
            hashtags_by_user[document['_id']] = document['hashtags_array']
        return hashtags_by_user

    def create_indexes(self):
        self.logger.info('Creating timestamp index for collection user_hashtag.')
        Mongo().get().db.user_hashtag.create_index('timestamp')
