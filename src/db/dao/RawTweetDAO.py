from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class RawTweetDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(RawTweetDAO, self).__init__(Mongo().get().db.raw_tweets)
        self.logger = Logger(self.__class__.__name__)

    def put(self, raw_tweet):
        """ Adds RawTweet to data base using upsert to update 'follows' list."""
        self.upsert({'id': raw_tweet.id},
                    {'$set': {'text': raw_tweet.text,
                              'created_at': raw_tweet.created_at,
                              'user_id': raw_tweet.user_id}
                     })
        #TODO crear un metodo para obtener y actualziar la cola