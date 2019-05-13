from unittest import TestCase, mock

import mongomock
from mock import MagicMock

from src.db.Mongo import Mongo
from src.db.dao.CandidatesFollowersDAO import CandidatesFollowersDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.exception.CredentialsAlreadyInUseError import CredentialsAlreadyInUseError
from src.exception.FollowerUpdatingNotNecessaryError import FollowerUpdatingNotNecessaryError
from src.model.Candidate import Candidate
from src.model.Credential import Credential
from src.service.candidates.CandidateService import CandidateService
from src.service.credentials.CredentialService import CredentialService
from src.service.followers.FollowerUpdateService import FollowerUpdateService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.config.ConfigurationManager import ConfigurationManager
from test.helpers.FollowerUpdateHelper import FollowerUpdateHelper


class TestFollowerUpdateService(TestCase):

    def setUp(self) -> None:
        # We need this to avoid mocking some object creations
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        FollowerUpdateHelper.ITERATIONS = 1
        FollowerUpdateHelper.CURRENT_ITERATION = 0

    @mock.patch.object(CredentialService, 'get_all_credentials_for_service', return_value={})
    @mock.patch.object(AsyncThreadPoolExecutor, 'run')
    def test_update_followers_ok(self, async_mock, credentials_mock):
        FollowerUpdateService.update_followers()
        assert credentials_mock.call_count == 1
        assert async_mock.call_count == 1

    @mock.patch.object(CredentialService, 'get_all_credentials_for_service',
                       **{'side_effect': CredentialsAlreadyInUseError('test')})
    @mock.patch.object(AsyncThreadPoolExecutor, 'run')
    def test_update_followers_credentials_exception(self, async_mock, credentials_mock):
        FollowerUpdateService.update_followers()
        assert credentials_mock.call_count == 1
        assert async_mock.call_count == 0

    @mock.patch.object(FollowerUpdateService, 'twitter', return_value={})
    @mock.patch.object(FollowerUpdateService, 'next_candidate', side_effect=FollowerUpdateHelper.mock_next_candidate)
    @mock.patch.object(FollowerUpdateService, 'update_followers_for_candidate')
    @mock.patch.object(CandidateService, 'finish_follower_updating')
    @mock.patch.object(CredentialService, 'unlock_credential')
    def test_update_with_credential_ok(self, unlock_mock, finish_mock, update_mock, next_mock, twitter_mock):
        credential = Credential(**{'ID': 'test', 'CONSUMER_KEY': 'test', 'CONSUMER_SECRET': 'test'})
        FollowerUpdateService.update_with_credential(credential)
        assert unlock_mock.call_count == 1
        assert finish_mock.call_count == 1
        assert update_mock.call_count == 1
        assert next_mock.call_count == 2
        assert twitter_mock.call_count == 1

    @mock.patch.object(FollowerUpdateService, 'twitter', return_value={})
    @mock.patch.object(FollowerUpdateService, 'next_candidate', side_effect=FollowerUpdateHelper.mock_next_candidate)
    @mock.patch.object(FollowerUpdateService, 'update_followers_for_candidate')
    @mock.patch.object(CandidateService, 'finish_follower_updating')
    @mock.patch.object(CredentialService, 'unlock_credential')
    def test_update_with_credential_no_candidates(self, unlock_mock, finish_mock, update_mock, next_mock, twitter_mock):
        FollowerUpdateHelper.ITERATIONS = 0
        credential = Credential(**{'ID': 'test', 'CONSUMER_KEY': 'test', 'CONSUMER_SECRET': 'test'})
        FollowerUpdateService.update_with_credential(credential)
        assert unlock_mock.call_count == 1
        assert finish_mock.call_count == 0
        assert update_mock.call_count == 0
        assert next_mock.call_count == 1
        assert twitter_mock.call_count == 1

    @mock.patch.object(RawFollowerDAO, 'get_candidate_followers_ids', return_value=[])
    @mock.patch.object(FollowerUpdateService, 'get_new_followers_ids', return_value=[])
    @mock.patch.object(FollowerUpdateService, 'store_new_followers')
    def test_update_followers_for_candidate_ok(self, store_mock, new_mock, previous_mock):
        twitter = MagicMock()
        candidate = Candidate(**{'screen_name': 'test'})
        FollowerUpdateService.update_followers_for_candidate(twitter, candidate)
        assert store_mock.call_count == 1
        assert new_mock.call_count == 1
        assert previous_mock.call_count == 1

    @mock.patch.object(FollowerUpdateService, 'should_retrieve_more_followers',
                       side_effect=FollowerUpdateHelper.mock_should_retrieve)
    @mock.patch.object(FollowerUpdateService, 'do_request', side_effect=FollowerUpdateHelper.mock_do_request)
    def test_get_new_followers_ids(self, request_mock, retrieve_mock):
        twitter = MagicMock()
        candidate = Candidate(**{'screen_name': 'test'})
        followers_ids = ['123', '456', '789']
        # Do test
        new_followers = FollowerUpdateService.get_new_followers_ids(twitter, candidate, followers_ids)
        assert retrieve_mock.call_count == 2
        assert request_mock.call_count == 2  # Once outside the while, once inside of it
        assert len(new_followers) == 4
        assert new_followers == {'012', '324', '678', '055'}

    @mock.patch.object(RawFollowerDAO, 'put')
    @mock.patch.object(CandidatesFollowersDAO, 'put_increase_for_candidate')
    def test_store_new_followers(self, increase_mock, put_mock):
        FollowerUpdateService.store_new_followers({'012', '324', '678', '055'}, 'test-name')
        assert put_mock.call_count == 4
        assert increase_mock.call_count == 1

    @mock.patch.object(ConfigurationManager, 'get_int', return_value=2)
    def test_should_retrieve_more_followers_true(self, get_mock):
        result = FollowerUpdateService.should_retrieve_more_followers({'012', '234', '463'}, {'012', '532', '987'})
        assert result
        assert get_mock.call_count == 1

    @mock.patch.object(ConfigurationManager, 'get_int', return_value=2)
    def test_should_retrieve_more_followers_false(self, get_mock):
        result = FollowerUpdateService.should_retrieve_more_followers({'012', '234', '463'}, {'012', '234', '987'})
        assert not result
        assert get_mock.call_count == 1

    @mock.patch.object(CandidateService, 'get_for_follower_updating', return_value=Candidate(**{'screen_name': 'test'}))
    def test_next_candidate_ok(self, get_mock):
        candidate = FollowerUpdateService.next_candidate()
        assert candidate is not None
        assert candidate.screen_name == 'test'
        assert get_mock.call_count == 1

    @mock.patch.object(CandidateService, 'get_for_follower_updating',
                       **{'side_effect': FollowerUpdatingNotNecessaryError()})
    def test_next_candidate_no_more_candidates_to_update(self, get_mock):
        candidate = FollowerUpdateService.next_candidate()
        assert candidate is None
        assert get_mock.call_count == 1

    @mock.patch('time.sleep', return_value=None)
    def test_do_request_ok(self, time_mock):
        twitter = MagicMock()
        twitter.get_followers_ids.side_effect = FollowerUpdateHelper.mock_get_followers_ids
        response = FollowerUpdateService.do_request(twitter, 'test', 0)
        assert response is not None
        assert response == {'ids': ['012', '324'], 'next_cursor': 678}
        assert twitter.get_followers_ids.call_count == 1
        assert time_mock.call_count == 0

    @mock.patch('time.sleep', return_value=None)
    def test_do_request_with_exception(self, time_mock):
        twitter = MagicMock()
        twitter.get_followers_ids.side_effect = FollowerUpdateHelper.mock_get_followers_ids_with_exception
        response = FollowerUpdateService.do_request(twitter, 'test', 0)
        assert response is not None
        assert response == {'ids': ['012', '324'], 'next_cursor': 678}
        assert twitter.get_followers_ids.call_count == 2
        assert time_mock.call_count == 1

    def test_twython_instance_creation_consumer_data(self):
        credential = Credential(**{'ID': 'test', 'CONSUMER_KEY': 'test', 'CONSUMER_SECRET': 'test'})
        twitter = FollowerUpdateService.twitter(credential)
        assert twitter.app_key is not None
        assert twitter.app_secret is not None
        assert twitter.oauth_token is None
        assert twitter.oauth_token_secret is None

    def test_twython_instance_creation_oauth_data(self):
        credential = Credential(**{'ID': 'test', 'ACCESS_TOKEN': 'test', 'ACCESS_SECRET': 'test'})
        twitter = FollowerUpdateService.twitter(credential)
        assert twitter.app_key is None
        assert twitter.app_secret is None
        assert twitter.oauth_token is not None
        assert twitter.oauth_token_secret is not None

    def test_twython_instance_creation_all_fields(self):
        credential = Credential(**{'ID': 'test', 'ACCESS_TOKEN': 'test', 'ACCESS_SECRET': 'test',
                                   'CONSUMER_KEY': 'test', 'CONSUMER_SECRET': 'test'})
        twitter = FollowerUpdateService.twitter(credential)
        assert twitter.app_key is not None
        assert twitter.app_secret is not None
        assert twitter.oauth_token is not None
        assert twitter.oauth_token_secret is not None
