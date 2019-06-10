import mongomock

from src.db.Mongo import Mongo
from src.db.dao.RawTweetDAO import RawTweetDAO
from test.meta.CustomTestCase import CustomTestCase


class TestRawTweetDAO(CustomTestCase):

    def setUp(self) -> None:
        super(TestRawTweetDAO, self).setUp()
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        self.target = RawTweetDAO()

    def tearDown(self) -> None:
        # This has to be done because we are testing a Singleton
        RawTweetDAO._instances.clear()

    def test_cooccurrence_checked(self):
        tweet = {'_id': 'some_id'}
        self.target.insert_tweet(tweet)
        self.target.cooccurrence_checked(tweet)
        retrieved = self.target.get_first({'_id': 'some_id'})
        assert retrieved is not None
        assert retrieved.get('cooccurrence_checked', None) is not None
        assert retrieved['cooccurrence_checked']

    def test_hashtag_origin_checked(self):
        tweet = {'_id': 'some_id'}
        self.target.insert_tweet(tweet)
        self.target.hashtag_origin_checked(tweet)
        retrieved = self.target.get_first({'_id': 'some_id'})
        assert retrieved is not None
        assert retrieved.get('hashtag_origin_checked', None) is not None
        assert retrieved['hashtag_origin_checked']
