from unittest import mock

import mongomock

from src.db.Mongo import Mongo
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.service.credentials.CredentialService import CredentialService
from src.service.tweets.TweetUpdateService import TweetUpdateService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from test.helpers.TweetUpdateHelper import TweetUpdateHelper
from test.meta.CustomTestCase import CustomTestCase


class TestTweetUpdateService(CustomTestCase):

    def setUp(self) -> None:
        super(TestTweetUpdateService, self).setUp()
        # We need this to avoid mocking some object creations
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)

    @mock.patch.object(CredentialService, 'get_all_credentials_for_service', return_value={})
    @mock.patch.object(AsyncThreadPoolExecutor, 'run')
    def test_update_tweets_ok(self, async_mock, credentials_mock):
        TweetUpdateService.update_tweets()
        assert credentials_mock.call_count == 1
        assert async_mock.call_count == 1

    def test_check_if_continue_downloading(self):
        tweet = TweetUpdateHelper().get_mock_tweet_may_26_follower_1()
        min_date = TweetUpdateHelper().get_mock_min_date_may_25()

        result = TweetUpdateService.check_if_continue_downloading(tweet, min_date)

        assert result == True

    def test_check_if_continue_downloading_return_false(self):
        tweet = TweetUpdateHelper().get_mock_tweet_may_24_follower_1()
        min_date = TweetUpdateHelper().get_mock_min_date_may_25()

        result = TweetUpdateService.check_if_continue_downloading(tweet, min_date)

        assert result == False

    @mock.patch.object(RawTweetDAO, 'put')
    def test_do_not_store_new_tweets(self, put_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        tweet2 = TweetUpdateHelper().get_mock_tweet_may_24_follower_1()
        download_tweets = [tweet2]
        min_date = TweetUpdateHelper().get_mock_min_date_may_25()

        TweetUpdateService.store_new_tweets(follower, download_tweets, min_date)

        assert put_mock.call_count == 0

    @mock.patch.object(RawTweetDAO, 'put')
    def test_store_part_of_new_tweets(self, put_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        tweet1 = TweetUpdateHelper().get_mock_tweet_may_26_follower_1()
        tweet2 = TweetUpdateHelper().get_mock_tweet_may_24_follower_1()
        download_tweets = [tweet1, tweet2]
        min_date = TweetUpdateHelper().get_mock_min_date_may_25()

        TweetUpdateService.store_new_tweets(follower, download_tweets, min_date)

        assert put_mock.call_count == 1

    @mock.patch.object(RawTweetDAO, 'put')
    def test_store_new_tweets(self, put_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        tweet1 = TweetUpdateHelper().get_mock_tweet_may_26_follower_1()
        tweet2 = TweetUpdateHelper().get_mock_tweet_may_24_follower_1()
        download_tweets = [tweet1, tweet2]
        min_date = TweetUpdateHelper().get_mock_min_date_may_24()

        TweetUpdateService.store_new_tweets(follower, download_tweets, min_date)

        assert put_mock.call_count == 2

    @mock.patch.object(TweetUpdateService, 'do_download_tweets_request', return_value=[])
    def test_download_tweets_with_no_results(self, download_tweets_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        is_first_request = True
        max_id = None

        TweetUpdateService.do_download_tweets_request(follower, is_first_request, max_id)

        assert download_tweets_mock.call_count == 1

    @mock.patch.object(TweetUpdateService, 'do_download_tweets_request', return_value=[])
    def test_do_download_tweets_requests_with_no_results(self, download_tweets_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        is_first_request = True
        max_id = None

        result = TweetUpdateService.do_download_tweets_request(follower, is_first_request, max_id)

        assert download_tweets_mock.call_count == 1
        assert len(result) == 0

    @mock.patch.object(TweetUpdateService, 'do_download_tweets_request',
                       return_value=[TweetUpdateHelper().get_mock_tweet_may_26_follower_1()])
    def test_do_download_tweets_requests_with_no_results(self, download_tweets_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        is_first_request = True
        max_id = None

        result = TweetUpdateService.do_download_tweets_request(follower, is_first_request, max_id)

        assert download_tweets_mock.call_count == 1
        assert len(result) == 1

    @mock.patch.object(TweetUpdateService, 'twitter', return_value={})
    @mock.patch.object(TweetUpdateService, 'do_download_tweets_request', return_value=[])
    def test_download_tweets_and_validate_with_no_results(self, download_tweets_mock, twitter_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        follower_download_tweets = []
        min_tweet_date = TweetUpdateHelper().get_mock_min_date_may_25()
        is_first_request = True
        max_id = None

        TweetUpdateService.download_tweets_and_validate(twitter_mock, follower, follower_download_tweets,
                                                        min_tweet_date, is_first_request, max_id)

        assert len(follower_download_tweets) == 0
        assert download_tweets_mock.call_count == 1

    @mock.patch.object(TweetUpdateService, 'twitter', return_value={})
    @mock.patch.object(TweetUpdateService, 'do_download_tweets_request',
                       return_value=[TweetUpdateHelper().get_mock_tweet_may_26_follower_1()])
    def test_download_tweets_and_validate_with_results(self, download_tweets_mock, twitter_mock):
        follower = TweetUpdateHelper().get_mock_follower_1()
        follower_download_tweets = []
        min_tweet_date = TweetUpdateHelper().get_mock_min_date_may_25()
        is_first_request = True
        max_id = None

        TweetUpdateService.download_tweets_and_validate(twitter_mock, follower, follower_download_tweets,
                                                        min_tweet_date, is_first_request, max_id)

        assert len(follower_download_tweets) == 1
        assert download_tweets_mock.call_count == 1
