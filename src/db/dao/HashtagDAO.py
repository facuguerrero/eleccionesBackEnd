from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class HashtagDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(HashtagDAO, self).__init__(Mongo().get().db.hashtags)
        self.logger = Logger(self.__class__.__name__)

    def find(self, hashtag_key):
        """ Get a hashtag document with the given hashtag_key. """
        return self.get_first({'_id': hashtag_key})

    def put(self, hashtag_key, tweet, original):
        """ Put new hashtag data with upsert modality. """
        if tweet:
            # Generate document data
            new_values = {'tweet_id': str(tweet['_id']),
                          'user_id': str(tweet['user_id']),
                          'created_at': tweet['created_at'],
                          'original': original}
            update_dict = {'$set': new_values, '$inc': {'appearances': 1}}
        else:
            update_dict = {'$inc': {'appearances': 1}}
        # Do upsert
        self.collection.find_and_modify(query={'_id': hashtag_key},
                                        update=update_dict,
                                        upsert=True)
