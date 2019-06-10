from unittest import mock

import mongomock

from src.db.Mongo import Mongo
from src.db.dao.CooccurrenceDAO import CooccurrenceDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.service.hashtags.HashtagCooccurrenceService import HashtagCooccurrenceService
from test.helpers.RawTweetHelper import RawTweetHelper
from test.meta.CustomTestCase import CustomTestCase


class TestHashtagCooccurrenceService(CustomTestCase):

    def setUp(self) -> None:
        super(TestHashtagCooccurrenceService, self).setUp()
        # We need this to avoid mocking some object creations
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        self.target = HashtagCooccurrenceService

    @mock.patch.object(RawTweetDAO, 'cooccurrence_checked')
    @mock.patch.object(CooccurrenceDAO, 'store')
    def test_process_tweet(self, store_mock, checked_mock):
        tweet = RawTweetHelper.common_raw_tweet()
        self.target.process_tweet(tweet)
        assert store_mock.call_count == 3
        assert checked_mock.call_count == 1

    @mock.patch.object(RawTweetDAO, 'cooccurrence_checked')
    @mock.patch.object(CooccurrenceDAO, 'store')
    def test_process_tweet_retweet(self, store_mock, checked_mock):
        tweet = RawTweetHelper.common_raw_retweet()
        self.target.process_tweet(tweet)
        assert store_mock.call_count == 0
        assert checked_mock.call_count == 1

    @mock.patch.object(RawTweetDAO, 'cooccurrence_checked')
    @mock.patch.object(CooccurrenceDAO, 'store')
    def test_process_tweet_one_hashtag(self, store_mock, checked_mock):
        tweet = RawTweetHelper.common_raw_tweet_one_hashtag()
        self.target.process_tweet(tweet)
        assert store_mock.call_count == 0
        assert checked_mock.call_count == 1

    @mock.patch.object(RawTweetDAO, 'cooccurrence_checked')
    @mock.patch.object(CooccurrenceDAO, 'store')
    def test_process_retweet_ten_hashtags(self, store_mock, checked_mock):
        tweet = RawTweetHelper.common_raw_tweet_ten_hashtags()
        self.target.process_tweet(tweet)
        assert store_mock.call_count == 0
        assert checked_mock.call_count == 1