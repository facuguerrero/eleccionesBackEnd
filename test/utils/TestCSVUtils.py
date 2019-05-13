from unittest import TestCase, mock
from os.path import abspath, join, dirname
import mongomock

from src.db.Mongo import Mongo
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.model.Candidate import Candidate
from src.util.CSVUtils import CSVUtils


class TestCSVUtils(TestCase):

    def setUp(self) -> None:
        # Mocking the whole database is not unit testing but we don't care because this is done to only to
        # make some mocking easier
        Mongo().db = mongomock.database.Database(mongomock.MongoClient(), 'elections', _store=None)
        # This has to be overridden to avoid reading the real file
        path = f"{abspath(join(dirname(__file__), '../'))}/resources/followers/%s_followers.csv"
        CSVUtils.FOLLOWERS_PATH_FORMAT = path

    @mock.patch.object(RawFollowerDAO, 'candidate_was_loaded', return_value=False)
    @mock.patch.object(RawFollowerDAO, 'put', return_value=None)
    @mock.patch.object(RawFollowerDAO, 'finish_candidate', return_value=None)
    def test_read_follower(self, finish_mock, put_mock, loaded_mock):
        CSVUtils.read_followers_for_candidate(Candidate(**{'screen_name': 'mauriciomacri', 'nickname': 'macri'}))
        assert loaded_mock.call_count == 1
        assert put_mock.call_count == 4
        assert finish_mock.call_count == 1
