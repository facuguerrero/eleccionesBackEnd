import csv
from os.path import join, abspath, dirname
from datetime import datetime

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.model.followers.RawFollower import RawFollower
from src.service.candidates.CandidateService import CandidateService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.logging.Logger import Logger


class CSVUtils:
    FOLLOWERS_PATH_FORMAT = f"{abspath(join(dirname(__file__), '../'))}/resources/followers/%s_followers.csv"
    DATE_FORMAT = '%Y-%m-%d'

    # Flag to avoid starting more than once the loading process
    __running = False

    @classmethod
    def load_followers(cls):
        with open('/home/facundoguerrero/prueba.csv', 'r') as fd:
            reader = csv.reader(fd, delimiter=',')
            for row in reader:
                follower = RawFollower(**{'id': row[0],
                                          'downloaded_on': datetime.strptime(row[1], CSVUtils.DATE_FORMAT),
                                          'follows': 'prueba'})
                RawFollowerDAO().put(follower)

    @classmethod
    def read_followers(cls):
        """ Read al .csv files containing follower data for each candidate. """
        if cls.__running: return
        cls.__running = True
        candidates = CandidateService().get_all()
        AsyncThreadPoolExecutor().run(cls.read_followers_for_candidate, candidates)
        cls.__running = False

    @classmethod
    def read_followers_for_candidate(cls, candidate):
        """ Read .csv file and load followers into database for specific candidate"""
        if RawFollowerDAO().candidate_was_loaded(candidate.screen_name):
            cls.get_logger().info(f'Candidate {candidate.screen_name} followers .csv file has already been loaded.')
            return
        cls.get_logger().info(f'Loading .csv file for {candidate.screen_name}.')
        # Generate file path and open file
        path = cls.FOLLOWERS_PATH_FORMAT % candidate.nickname
        with open(path, 'r') as fd:
            reader = csv.reader(fd, delimiter=',')
            # Skip title
            title = next(reader)
            # Load followers
            for row in reader:
                # There are some cases were we have a second row with a title, so we'll skip it
                if row == title: continue
                follower = RawFollower(**{'id': row[0],
                                          'downloaded_on': datetime.strptime(row[1], CSVUtils.DATE_FORMAT),
                                          'follows': candidate.screen_name})
                RawFollowerDAO().put(follower)
        # Mark this candidate as already loaded.
        RawFollowerDAO().finish_candidate(candidate.screen_name)
        cls.get_logger().info(f'Finished loading {candidate.screen_name} raw followers from .csv file.')

    @classmethod
    def get_logger(cls):
        return Logger('CSVUtils')
