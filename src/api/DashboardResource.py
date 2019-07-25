from flask_restful import Resource

from src.db.dao.CooccurrenceDAO import CooccurrenceDAO
from src.db.dao.HashtagDAO import HashtagDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.service.dashboard.DashboardService import DashboardService
from src.util.ResponseBuilder import ResponseBuilder


class DashboardResource(Resource):

    @staticmethod
    def get():
        # Get count of analyzed tweets
        tweets = RawTweetDAO().get_count({})
        # Get count of known hashtags
        hashtags = HashtagDAO().get_count({})
        # Get count of hashtag cooccurrences
        cooccurrences = CooccurrenceDAO().get_count({})
        # Retrieve dashboard data from database
        dashboard_data = DashboardService.dashboard_data()
        # Build response object
        response = {'tweets': tweets,
                    'total_users': dashboard_data['users'],
                    'active_users': dashboard_data['active_users'],
                    'active_proportion': dashboard_data['active_proportion'],
                    'followers_by_candidate': dashboard_data['followers_by_candidate'],
                    'topic_count': dashboard_data['topics'],
                    'hashtag_count': hashtags,
                    'cooccurrences_count': cooccurrences}
        # Respond request
        return ResponseBuilder.build(response, 200)
