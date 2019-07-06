import datetime

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
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
        date = datetime.datetime.today() - datetime.timedelta(days=3)
        return self.get_all({'timestamp': {'$lt': date}})

    def get_last_3_days_users_and_hashtags(self, all_hashtags_sorted):
        """ Get las 3 days hashtag's list. """
        users_hashtags = self.aggregate_last_3_days_data()
        users_index = {}
        index = 0
        position_vectors = []

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

        return position_vectors

    def aggregate_last_3_days_data(self):
        """ Get iterator of last 3 days user-hashtags aggregated. """
        date = datetime.datetime.today() - datetime.timedelta(days=3)
        return self.aggregate([
            {'$match':
                 {'timestamp':
                      {'$lt': date}
                  }
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
