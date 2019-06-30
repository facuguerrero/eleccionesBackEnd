from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class UserHashtagDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(UserHashtagDAO, self).__init__(Mongo().get().db.user_hashtag)
        self.logger = Logger(self.__class__.__name__)

    def create_indexes(self):
        self.logger.info('Creating timestamp index for collection user_hashtag.')
        Mongo().get().db.user_hashtag.create_index('timestamp')
