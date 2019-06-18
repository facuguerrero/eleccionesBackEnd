from src.model.followers.RawFollower import RawFollower
from src.service.tweets.TweetUpdateService import TweetUpdateService


class TweetUpdateHelper:

    @staticmethod
    def get_mock_tweet_may_26_follower_1():
        return {'_id': '1',
                'created_at': 'Sun May 26 02:43:14 +0000 2019',
                'text': '',
                'user': {'id_str': '1',
                         'location': 'dummy',
                         'followers_count': 'dummy',
                         'friends_count': 'dummy',
                         'listed_count': 'dummy',
                         'favourites_count': 'dummy',
                         'statuses_count': 'dummy'
                         }
                }

    @staticmethod
    def get_mock_tweet_may_24_follower_1():
        return {'_id': '1',
                'created_at': 'Fri May 24 02:43:14 +0000 2019',
                'text': '',
                'user': {'id_str': '1'}
                }

    @staticmethod
    def get_mock_min_date_may_25():
        return TweetUpdateService.get_formatted_date('Sat May 25 02:43:14 +0000 2019')

    @staticmethod
    def get_mock_min_date_may_24():
        return TweetUpdateService.get_formatted_date('Fri May 24 00:00:00 +0000 2019')

    @staticmethod
    def get_mock_follower_1():
        return '1'

    @staticmethod
    def get_mock_follower_private():
        return RawFollower(**{'id': 'dummy', 'is_private': True})

    @staticmethod
    def get_mock_follower_not_private():
        return RawFollower(**{'id': 'dummy', 'is_private': False})

    @staticmethod
    def get_twitter_mock():
        return RawFollower(**{'id': 'dummy', 'is_private': False})
