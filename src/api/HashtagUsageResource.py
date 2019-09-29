from datetime import datetime

from flask import request
from flask_restful import Resource

from src.service.hashtags.HashtagUsageService import HashtagUsageService


class HashtagUsageResource(Resource):

    @staticmethod
    def post():
        date = datetime.combine(datetime.strptime(request.args.get('date'), '%Y-%m-%d').date(), datetime.min.time())
        HashtagUsageService.calculate_topics_hashtag_usage(date)
