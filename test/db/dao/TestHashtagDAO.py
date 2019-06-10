import mongomock

from src.db.Mongo import Mongo
from src.db.dao.HashtagDAO import HashtagDAO
from test.helpers.RawTweetHelper import RawTweetHelper
from test.meta.CustomTestCase import CustomTestCase


class TestHashtagDAO(CustomTestCase):
    
    def setUp(self) -> None:
        super(TestHashtagDAO, self).setUp()
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        self.target = HashtagDAO()

    def tearDown(self) -> None:
        # This has to be done because we are testing a Singleton
        HashtagDAO._instances.clear()

    def test_put(self):
        tweet = RawTweetHelper.common_raw_tweet()
        self.target.put('emperor', tweet, 'Emperor')
        hashtag = self.target.find('emperor')
        assert hashtag is not None
        assert hashtag['tweet_id'] == tweet['_id']
        assert hashtag['user_id'] == tweet['user_id']
        assert hashtag['created_at'] == tweet['created_at']
        assert hashtag['original'] == 'Emperor'
