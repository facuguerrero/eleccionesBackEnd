from twython import TwythonRateLimitError

from src.model.Candidate import Candidate


class FollowerUpdateHelper:
    ITERATIONS = 1
    NEXT_CANDIDATE_ITERATION = 0
    SHOULD_RETRIEVE_ITERATION = 0
    FOLLOWERS_ITERATION = 0

    @classmethod
    def restart_all_iterations(cls):
        cls.ITERATIONS = 1
        cls.SHOULD_RETRIEVE_ITERATION = 0
        cls.NEXT_CANDIDATE_ITERATION = 0
        cls.FOLLOWERS_ITERATION = 0

    @staticmethod
    def mock_next_candidate():
        """ Returns a candidate NEXT_CANDIDATE_ITERATIONS times. Then, it returns None. """
        if FollowerUpdateHelper.NEXT_CANDIDATE_ITERATION >= FollowerUpdateHelper.ITERATIONS:
            return None
        FollowerUpdateHelper.NEXT_CANDIDATE_ITERATION += 1
        return Candidate(**{'screen_name': 'test'})

    @staticmethod
    def mock_should_retrieve(previous, new):
        """ Returns True or False mocking if there should be more retrieving steps. """
        if FollowerUpdateHelper.SHOULD_RETRIEVE_ITERATION >= FollowerUpdateHelper.ITERATIONS:
            return False
        FollowerUpdateHelper.SHOULD_RETRIEVE_ITERATION += 1
        return True

    @staticmethod
    def mock_do_request(twitter, screen_name, cursor=0):
        """ Returns a mock response for Twython's get_followers_ids. """
        if cursor == 0:
            return {'ids': [12, 324], 'next_cursor': 678}
        else:
            return {'ids': [678, 55], 'next_cursor': 901}

    @staticmethod
    def mock_get_followers_ids(screen_name, cursor=0):
        return FollowerUpdateHelper.mock_do_request({}, screen_name, int(cursor))

    @staticmethod
    def mock_get_followers_ids_with_exception(screen_name, cursor=0):
        if FollowerUpdateHelper.FOLLOWERS_ITERATION >= FollowerUpdateHelper.ITERATIONS:
            return FollowerUpdateHelper.mock_do_request({}, screen_name, int(cursor))
        FollowerUpdateHelper.FOLLOWERS_ITERATION += 1
        raise TwythonRateLimitError('message', 409)
