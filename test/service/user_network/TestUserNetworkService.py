from unittest import mock

import mongomock
from twython import TwythonRateLimitError

from src.db.Mongo import Mongo
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.model.Credential import Credential
from src.service.user_network.UserNetworkRetrievalService import UserNetworkRetrievalService
from src.util.twitter.TwitterUtils import TwitterUtils
from test.meta.CustomTestCase import CustomTestCase
from test.meta.JsonLoader import JsonLoader


class TestUserNetworkRetrievalService(CustomTestCase):

    credential = Credential(**{'ID': 'test', 'CONSUMER_KEY': 'key', 'CONSUMER_SECRET': 'secret'})

    def setUp(self) -> None:
        super(TestUserNetworkRetrievalService, self).setUp()
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)

    class MockedTwython:

        def __init__(self, file_names):
            self.file_names = file_names
            self.current_index = 0
            self.fail_on_index = -1
            self.exception = None

        def get_friends_ids(self, user_id, stringify_ids, cursor):
            if self.current_index == self.fail_on_index:
                self.fail_on_index = -1
                raise self.exception
            response = JsonLoader.json_from_string_resource(self.file_names[self.current_index])
            self.current_index += 1
            return response

    @mock.patch.object(TwitterUtils, 'twitter')
    def test_do_download(self, mocked_twitter):
        fake_twython = self.MockedTwython(['friends_ids_end'])
        mocked_twitter.return_value = fake_twython

        friends = UserNetworkRetrievalService.do_download(user_id='12345', cursor=0, credential=self.credential)
        assert fake_twython.current_index == 1

        fake_twython.current_index = 0
        assert friends == set(fake_twython.get_friends_ids("", "", "").get('ids'))

    @mock.patch.object(TwitterUtils, 'twitter')
    def test_do_download_recursive(self, mocked_twitter):
        fake_twython = self.MockedTwython(['friends_ids_cursor0', 'friends_ids_cursor_next', 'friends_ids_end'])
        mocked_twitter.return_value = fake_twython

        friends = UserNetworkRetrievalService.do_download(user_id='12345', cursor=0, credential=self.credential)
        assert fake_twython.current_index == 3

        fake_twython.current_index = 0
        set1 = set(fake_twython.get_friends_ids("", "", "").get('ids'))
        set2 = set(fake_twython.get_friends_ids("", "", "").get('ids'))
        set3 = set(fake_twython.get_friends_ids("", "", "").get('ids'))
        assert friends == set1.union(set2.union(set3))

    @mock.patch.object(TwitterUtils, 'twitter')
    @mock.patch('time.sleep', return_value=None)
    def test_do_download_recursive_with_error(self, mocked_sleep, mocked_twitter):
        fake_twython = self.MockedTwython(['friends_ids_cursor0', 'friends_ids_cursor_next', 'friends_ids_end'])
        mocked_twitter.return_value = fake_twython

        fake_twython.fail_on_index = 2
        fake_twython.exception = TwythonRateLimitError("", "")
        friends = UserNetworkRetrievalService.do_download(user_id='12345', cursor=0, credential=self.credential)
        assert fake_twython.current_index == 3
        assert mocked_sleep.call_count == 1

        fake_twython.current_index = 0
        set1 = set(fake_twython.get_friends_ids("", "", "").get('ids'))
        set2 = set(fake_twython.get_friends_ids("", "", "").get('ids'))
        set3 = set(fake_twython.get_friends_ids("", "", "").get('ids'))
        assert friends == set1.union(set2.union(set3))

    def test_retrieve_users_by_party(self):
        document = {
            '_id': '123',
            'is_private': False,
            'has_tweets': True,
            'probability_vector_support': [0.8],
            'support': 'juntosporelcambio',
            'friends_count': 2500
        }
        RawFollowerDAO().insert(document)
        document = {
            '_id': '456',
            'is_private': False,
            'has_tweets': True,
            'probability_vector_support': [0.8],
            'support': 'frentedetodos',
            'friends_count': 2500
        }
        RawFollowerDAO().insert(document)
        document = {
            '_id': '789',
            'is_private': False,
            'has_tweets': True,
            'probability_vector_support': [0.8],
            'support': 'frentedetodos',
            'friends_count': 5001
        }
        RawFollowerDAO().insert(document)
        result = UserNetworkRetrievalService.retrieve_users_by_party()
        assert len(result['juntosporelcambio']) == 1
        assert result['juntosporelcambio'][0] == '123'
        assert len(result['frentedetodos']) == 1
        assert result['frentedetodos'][0] == '456'
