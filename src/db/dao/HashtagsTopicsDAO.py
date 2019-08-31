import datetime

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.DateUtils import DateUtils
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

    def get_required_hashtags(self, all_hashtags, hashtags_index, date):
        """ Retrieve all topics by hashtags. """
        init_first_hour, yesterday_last_hour = self.get_init_and_end_dates(date)
        self.logger.info(init_first_hour)
        self.logger.info(yesterday_last_hour)
        self.logger.info(len(all_hashtags))

        hashtags_topics = self.get_all({'$and': [
            {'start_date': init_first_hour},
            {'end_date': yesterday_last_hour},
            {'hashtag': {'$in': all_hashtags}}
        ]})

        position_vectors = []
        for hashtag_topics in hashtags_topics:
            hashtag = hashtag_topics['hashtag']
            hashtag_index = hashtags_index[hashtag]

            for topic in hashtag_topics['topics']:
                position_vectors.append([hashtag_index, int(topic), 1])
        self.logger.info(len(position_vectors))
        return position_vectors

    @staticmethod
    def get_init_and_end_dates(date):
        """ Return 10 days ago at 00:00 and yesterday at 23:59"""
        init_date = date - datetime.timedelta(days=11)
        init_first_hour = DateUtils().date_at_first_hour(init_date)

        yesterday = date - datetime.timedelta(days=1)
        yesterday_last_hour = DateUtils().date_at_last_hour(yesterday)

        return init_first_hour, yesterday_last_hour
