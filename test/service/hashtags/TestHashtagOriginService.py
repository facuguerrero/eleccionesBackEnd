from unittest import mock
from datetime import datetime

import mongomock

from src.db.Mongo import Mongo
from src.db.dao.HashtagDAO import HashtagDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.service.hashtags.HashtagOriginService import HashtagOriginService
from test.helpers.RawTweetHelper import RawTweetHelper
from test.meta.CustomTestCase import CustomTestCase


class TestHashtagOriginService(CustomTestCase):

    def setUp(self) -> None:
        super(TestHashtagOriginService, self).setUp()
        # We need this to avoid mocking some object creations
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        self.target = HashtagOriginService

    @mock.patch.object(RawTweetDAO, 'hashtag_origin_checked')
    @mock.patch.object(HashtagDAO, 'find', return_value=None)
    @mock.patch.object(HashtagDAO, 'put')
    def test_process_tweet_no_previous(self, put_mock, find_mock, checked_mock):
        tweet = RawTweetHelper.common_raw_tweet()
        self.target.process_tweet(tweet)
        assert find_mock.call_count == 3
        assert put_mock.call_count == 3
        assert checked_mock.call_count == 1

    @mock.patch.object(RawTweetDAO, 'hashtag_origin_checked')
    @mock.patch.object(HashtagDAO, 'find', return_value={'created_at': datetime.strptime('2019-05-23', '%Y-%m-%d')})
    @mock.patch.object(HashtagDAO, 'put')
    def test_process_tweet_previous_newer(self, put_mock, find_mock, checked_mock):
        tweet = RawTweetHelper.common_raw_tweet()
        self.target.process_tweet(tweet)
        assert find_mock.call_count == 3
        assert put_mock.call_count == 3
        assert checked_mock.call_count == 1

    @mock.patch.object(RawTweetDAO, 'hashtag_origin_checked')
    @mock.patch.object(HashtagDAO, 'find', return_value={'created_at': datetime.strptime('2019-05-21', '%Y-%m-%d')})
    @mock.patch.object(HashtagDAO, 'put')
    def test_process_tweet_previous_older(self, put_mock, find_mock, checked_mock):
        tweet = RawTweetHelper.common_raw_tweet()
        self.target.process_tweet(tweet)
        assert find_mock.call_count == 3
        assert put_mock.call_count == 0
        assert checked_mock.call_count == 1
