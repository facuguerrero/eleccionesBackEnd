import datetime

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.util.DateUtils import DateUtils
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class UserHashtagDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(UserHashtagDAO, self).__init__(Mongo().get().db.user_hashtag)
        self.logger = Logger(self.__class__.__name__)
        self.user_hashtags_count = {}

    def get_last_10_days_hashtags(self, date):
        """ Get las 10 days hashtag's list. """
        users_hashtags = self.retrieve_last_10_days_data(date)

        hashtags = set()
        users = set()
        self.user_hashtags_count = {}

        for user_hashtag in users_hashtags:
            user = str(user_hashtag['user'])
            hashtag = user_hashtag['hashtag']

            key = (user, hashtag)
            hashtags.add(hashtag)
            users.add(user)

            self.user_hashtags_count[key] = self.user_hashtags_count[key] + 1 if key in self.user_hashtags_count else 1
        return sorted(list(hashtags)), users

    def retrieve_last_10_days_data(self, date):
        """ Get iterator of last 10 days user-hashtags. """
        users_to_be_discarded = RawFollowerDAO().get_all({'important': False}, {'_id': 1})
        ids = []
        for user in users_to_be_discarded:
            ids.append(user['_id'])
        self.logger.info(f'Users discarded: {len(ids)}')

        init_first_hour, yesterday_last_hour = self.get_init_and_end_dates(date)
        return self.get_all({'$and': [
            {'timestamp': {'$gte': init_first_hour}},
            {'timestamp': {'$lte': yesterday_last_hour}},
            {'user': {'$nin': ids}}
        ]})

    @staticmethod
    def get_init_and_end_dates(date=datetime.datetime.today()):
        """ Return 3 days ago at 00:00 and yesterday at 23:59"""
        init_date = date - datetime.timedelta(days=11)
        init_first_hour = DateUtils().date_at_first_hour(init_date)

        yesterday = date - datetime.timedelta(days=1)
        yesterday_last_hour = DateUtils().date_at_last_hour(yesterday)

        return init_first_hour, yesterday_last_hour

    def get_last_10_days_users_and_hashtags(self, all_hashtags_sorted):
        """ Get las 3 days hashtag's list from cached data. """
        users_index = {}
        index = 0
        position_vectors = []

        self.logger.info(f'Users-Hashtags quantity: {len(self.user_hashtags_count)}')
        for user_hashtag_tuple in self.user_hashtags_count.keys():
            count = self.user_hashtags_count[user_hashtag_tuple]
            user = user_hashtag_tuple[0]

            if user in users_index:
                user_index = users_index[user]
            else:
                user_index = index
                users_index[user] = user_index
                index += 1

            hashtag = user_hashtag_tuple[1]
            hashtag_index = all_hashtags_sorted[hashtag]
            position_vectors.append([user_index, hashtag_index, count])

        return position_vectors, users_index

    def aggregate_last_3_days_data(self):
        """ Get iterator of last 3 days user-hashtags aggregated. """
        # TODO Delete method
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
        # TODO Delete method.
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
