from src.db.dao.CooccurrenceGraphDAO import CooccurrenceGraphDAO
from src.db.dao.DashboardDAO import DashboardDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.service.candidates.CandidateService import CandidateService


class DashboardService:

    @staticmethod
    def update_dashboard_data():
        """ Recalculate non-counting dashboard data and store. """
        # Get total count of users
        users = RawFollowerDAO().get_count({})
        # Get count of active users
        active_users = RawFollowerDAO().get_count({'has_tweets': True})
        # Get count of followers for each candidate
        candidates = list(map(lambda c: c.screen_name, CandidateService().get_all()))
        followers_by_candidate = dict()
        for candidate in candidates:
            followers = RawFollowerDAO().get_count({'follows': candidate})
            active_followers = RawFollowerDAO().get_count({'follows': candidate, 'has_tweets': True})
            followers_by_candidate[candidate] = {'followers': followers,
                                                 'active_followers': active_followers,
                                                 'proportion': active_followers / followers}
        # Get count of found topics
        topics = CooccurrenceGraphDAO().get_count({'topic_id': {'$ne': 'main'}})
        DashboardDAO().store({
            'users': users,
            'active_users': active_users,
            'active_proportion': active_users / users,
            'followers_by_candidate': followers_by_candidate,
            'topics': topics})
