import csv
from os.path import join, abspath, dirname
from datetime import datetime

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.model.followers.RawFollower import RawFollower
from src.service.candidates.CandidateService import CandidateService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.logging.Logger import Logger


class CSVUtils:
    LOGGER = Logger(__name__)
    FOLLOWERS_PATH_FORMAT = f"{abspath(join(dirname(__file__), '../'))}/resources/followers/%s_followers.csv"
    DATE_FORMAT = '%Y-%m-%d'

    @classmethod
    def read_followers(cls):
        candidates = CandidateService().get_all()
        AsyncThreadPoolExecutor().run(cls.read_followers_for_candidate, candidates)

    @classmethod
    def read_followers_for_candidate(cls, candidate):
        if RawFollowerDAO().candidate_was_loaded(candidate.screen_name):
            cls.LOGGER.info(f'Candidate {candidate.screen_name} has already been loaded.')
            return
        cls.LOGGER.info(f'Loading .csv file for {candidate.screen_name}.')
        # Generate file path and open file
        path = cls.FOLLOWERS_PATH_FORMAT % candidate.nickname
        with open(path, 'r') as fd:
            reader = csv.reader(fd, delimiter=',')
            # Skip title
            title = next(reader)
            # Load followers
            for row in reader:
                # There are some cases were we have a second row with a title
                if row == title: continue
                follower = RawFollower(**{'id': row[0],
                                          'downloaded_on': datetime.strptime(row[1], CSVUtils.DATE_FORMAT),
                                          'follows': candidate.screen_name})
                RawFollowerDAO().put(follower)
        # Mark this candidate as already loaded.
        RawFollowerDAO().finish_candidate(candidate.screen_name)
        cls.LOGGER.info(f'Finished loading {candidate.screen_name} raw followers from .csv file.')
