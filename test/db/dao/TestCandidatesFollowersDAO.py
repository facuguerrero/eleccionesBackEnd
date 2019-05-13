import mongomock

from unittest import TestCase
from datetime import datetime

from src.db.Mongo import Mongo
from src.db.dao.CandidatesFollowersDAO import CandidatesFollowersDAO
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.util.CSVUtils import CSVUtils


class TestCandidatesFollowersDAO(TestCase):
    """ This set of tests will be more integrating than unit. """

    def setUp(self) -> None:
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
        assert increases[0]['date'] == test_date

    def test_put_increase_for_candidate_existing_candidate(self):
        # Set Up
        old_date = datetime.strptime("1996-03-15", CSVUtils.DATE_FORMAT)
        self.target.put_increase_for_candidate('test-name', 1000, old_date)
        # New test
        new_date = datetime.strptime("1901-05-25", CSVUtils.DATE_FORMAT)
        self.target.put_increase_for_candidate('test-name', 4000, new_date)
        increases = self.target.get_increases_for_candidate('test-name')
        assert len(increases) == 2
        assert increases[0]['count'] == 1000
        assert increases[0]['date'] == old_date
        assert increases[1]['count'] == 4000
        assert increases[1]['date'] == new_date

    def test_get_increases_for_candidate_raises_exception_with_no_entries(self):
        with self.assertRaises(NoDocumentsFoundError) as context:
            self.target.get_increases_for_candidate('test-name')
        assert context.exception is not None
        message = 'No documents found on collection candidates_followers with query screen_name=test-name.'
        assert context.exception.message == message
