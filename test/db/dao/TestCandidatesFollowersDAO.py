import mongomock

from datetime import datetime

from src.db.Mongo import Mongo
from src.db.dao.CandidatesFollowersDAO import CandidatesFollowersDAO
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.util.CSVUtils import CSVUtils
from src.util.DateUtils import DateUtils
from test.meta.CustomTestCase import CustomTestCase


class TestCandidatesFollowersDAO(CustomTestCase):
    """ This set of tests will be more integrating than unit. """

    def setUp(self) -> None:
        super(TestCandidatesFollowersDAO, self).setUp()
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        self.target = CandidatesFollowersDAO()

    def tearDown(self) -> None:
        # This has to be done because we are testing a Singleton
        CandidatesFollowersDAO._instances.clear()

    def test_put_increase_for_candidate_new_candidate(self):
        test_date = datetime.strptime("1996-03-15", CSVUtils.DATE_FORMAT)
        self.target.put_increase_for_candidate('test-name', 1000, test_date)
        increases = self.target.get_increases_for_candidate('test-name')
        assert len(increases) == 1
        assert increases[0]['count'] == 1000
        assert increases[0]['date'] == test_date.timestamp()

    def test_put_increase_for_candidate_existing_candidate(self):
        # Set Up
        old_date = datetime.strptime("1996-03-15", CSVUtils.DATE_FORMAT)
        self.target.put_increase_for_candidate('test-name', 1000, old_date)
        new_date = datetime.strptime("1901-05-25", CSVUtils.DATE_FORMAT)
        self.target.put_increase_for_candidate('test-name', 4000, new_date)
        # New test
        increases = self.target.get_increases_for_candidate('test-name')
        assert len(increases) == 2
        assert increases[0]['count'] == 1000
        assert increases[0]['date'] == old_date.timestamp()
        assert increases[1]['count'] == 4000
        assert increases[1]['date'] == new_date.timestamp()

    def test_get_increases_for_candidate_raises_exception_with_no_entries(self):
        with self.assertRaises(NoDocumentsFoundError) as context:
            self.target.get_increases_for_candidate('test-name')
        assert context.exception is not None
        message = 'No documents found on collection candidates_followers with query screen_name=test-name.'
        assert context.exception.message == message

    def test_get_all_increases(self):
        # Set Up
        old_date = datetime.strptime("1996-03-15", CSVUtils.DATE_FORMAT)
        self.target.put_increase_for_candidate('test1', 1000, old_date)
        self.target.put_increase_for_candidate('test2', 2400, old_date)
        new_date = datetime.strptime("1901-05-25", CSVUtils.DATE_FORMAT)
        self.target.put_increase_for_candidate('test1', 3000, new_date)
        self.target.put_increase_for_candidate('test2', 4000, new_date)
        # New test
        increases = self.target.get_all_increases()
        assert len(increases) == 2
        test1_increases = increases[0]
        assert len(test1_increases) == 3
        assert test1_increases['date'] == DateUtils.date_to_timestamp(old_date.date())
        assert test1_increases['test1'] == 1000
        assert test1_increases['test2'] == 2400
        test2_increases = increases[1]
        assert len(test2_increases) == 3
        assert test2_increases['date'] == DateUtils.date_to_timestamp(new_date.date())
        assert test2_increases['test1'] == 3000
        assert test2_increases['test2'] == 4000
