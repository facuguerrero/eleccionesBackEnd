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

    def get_required_hashtags(self, all_hashtags, all_topics_sorted):
        """ Retrieve all topics by hashtags. """
        init_first_hour, yesterday_last_hour = self.get_init_and_end_dates()
        count = hashtags_topics = self.get_all({'$and': [
            {'start_date': init_first_hour},
            {'end_date': yesterday_last_hour}
        ]}).count()
        self.logger.info(f"Init {init_first_hour}")
        self.logger.info(f"End {yesterday_last_hour}")
        self.logger.info(f"Count {count}")

        hashtags_topics = self.get_all({'$and': [
            {'start_date': init_first_hour},
            {'end_date': yesterday_last_hour}
        ]})

        position_vectors = []
        x = 0
        for hashtag_topics in hashtags_topics:
            x += 1
            if x % 10000 == 0:
                self.logger.info("Hashtags topics retrieved")
            # TODO esto no deberia estar
            # if hashtag_topics['hashtag'] in all_hashtags:
            hashtag_index = all_hashtags.index(hashtag_topics['hashtag'])
            for topic in hashtag_topics['topics']:
                position_vectors.append([hashtag_index, int(topic), 1])
        return position_vectors

    @staticmethod
    def get_init_and_end_dates():
        """ Return 3 days ago at 00:00 and yesterday at 23:59"""
        init_first_hour = datetime.datetime(2019, 1, 1, 0, 0, 0)
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday_last_hour = DateUtils().date_at_last_hour(yesterday)
        return init_first_hour, yesterday_last_hour
