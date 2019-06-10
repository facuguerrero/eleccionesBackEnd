import mongomock

from src.db.Mongo import Mongo
from src.db.dao.CooccurrenceDAO import CooccurrenceDAO
from test.helpers.RawTweetHelper import RawTweetHelper
from test.meta.CustomTestCase import CustomTestCase


class TestCooccurrenceDAO(CustomTestCase):

    def setUp(self) -> None:
        super(TestCooccurrenceDAO, self).setUp()
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        self.target = CooccurrenceDAO()

    def tearDown(self) -> None:
        # This has to be done because we are testing a Singleton
        CooccurrenceDAO._instances.clear()

    def test_store(self):
        tweet = RawTweetHelper.common_raw_tweet()
        self.target.store(tweet, ['Emperor', 'Caniggia'])
        cooccurrence = self.target.get_first({})
        assert cooccurrence is not None
        assert cooccurrence['user_id'] == tweet['user_id']
        assert cooccurrence['created_at'] == tweet['created_at']
        assert cooccurrence['pair'] == ['Emperor', 'Caniggia']
