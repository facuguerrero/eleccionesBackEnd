from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class UsersFriendsDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(UsersFriendsDAO, self).__init__(Mongo().get().db.users_friends)
        self.logger = Logger(self.__class__.__name__)

    def store_friends_for_user(self, user_id, party, friends):
        self.insert({'_id': user_id, 'friends': list(friends), 'party': party})

    def get_users_for_party(self, party):
        documents = self.get_all({'party': party}, {'_id': 0, 'friends': 1})
        return [document['friends'] for document in documents]
