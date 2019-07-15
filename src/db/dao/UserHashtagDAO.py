import datetime

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.DateUtils import DateUtils
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class UserHashtagDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(UserHashtagDAO, self).__init__(Mongo().get().db.user_hashtag)
        self.logger = Logger(self.__class__.__name__)

    def get_last_3_days_hashtags(self):
        """ Get las 3 days hashtag's list. """
        users_hashtags = self.retrieve_last_3_days_data()
        hashtags = set()
        for user_hashtag in users_hashtags:
            hashtags.add(user_hashtag['hashtag'])
        return sorted(list(hashtags))

    def retrieve_last_3_days_data(self):
        """ Get iterator of last 3 days user-hashtags. """
        init_first_hour, yesterday_last_hour = self.get_init_and_end_dates()

        return self.get_all({'$and': [
            {'timestamp': {'$gte': init_first_hour}},
            {'timestamp': {'$lte': yesterday_last_hour}}
        ]})

    @staticmethod
    def get_init_and_end_dates():
        # TODO cambiar por 3 dias
        init_date = datetime.datetime.today() - datetime.timedelta(days=1)
        init_first_hour = DateUtils().date_at_first_hour(init_date)
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday_last_hour = DateUtils().date_at_last_hour(yesterday)

        return init_first_hour, yesterday_last_hour

    def get_last_3_days_users_and_hashtags(self, all_hashtags_sorted):
        """ Get las 3 days hashtag's list. """
        users_index = {}
        index = 0
        position_vectors = []

        users_hashtags = self.aggregate_last_3_days_data()
        for user_hashtag in users_hashtags:
            user = user_hashtag['_id']['user']
            if user in users_index:
                user_index = users_index[user]
            else:
                user_index = index
                users_index[user] = user_index
                index += 1

            hashtag_index = all_hashtags_sorted.index(user_hashtag['_id']['hashtag'])
            count = user_hashtag['count']
            position_vectors.append([user_index, hashtag_index, count])

        return position_vectors, users_index

    def aggregate_last_3_days_data(self):
        """ Get iterator of last 3 days user-hashtags aggregated. """
        init_first_hour, yesterday_last_hour = self.get_init_and_end_dates()
        return self.aggregate([
            {'$match':
                {'$and': [
                    {'timestamp': {'$gte': init_first_hour}},
                    {'timestamp': {'$lte': yesterday_last_hour}}
                ]}
             },
            {'$group': {
                '_id': {
                    'user': '$user',
                    'hashtag': '$hashtag'
                },
                'count': {'$sum': 1}
            }
            }
        ])

    def get_yesterday_hashtags(self):
        # FILTRAR POR LOS ULTIMOS 3 DIAS
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
        all_hashtags = set()
        for document in documents:
            hashtags = document['hashtags_array']
            for hashtag in hashtags:
                all_hashtags.add(hashtag)
            hashtags_by_user[document['_id']] = hashtags
        hashtags_list = list(all_hashtags)
        return hashtags_by_user, sorted(hashtags_list)

    def create_indexes(self):
        self.logger.info('Creating timestamp index for collection user_hashtag.')
        Mongo().get().db.user_hashtag.create_index('timestamp')
