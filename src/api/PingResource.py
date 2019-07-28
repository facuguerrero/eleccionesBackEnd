from flask_restful import Resource

from src.service.hashtags.HashtagUsageService import HashtagUsageService
from src.util.ResponseBuilder import ResponseBuilder


class PingResource(Resource):

    @staticmethod
    def get():
        return ResponseBuilder.build(f'Ping.', 200)

    @staticmethod
    def post():
        HashtagUsageService.calculate_today_topics_hashtag_usage()
