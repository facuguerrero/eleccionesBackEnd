from datetime import datetime, timedelta

from flask import request
from flask_restful import Resource

from src.service.hashtags.HashtagUsageService import HashtagUsageService
from src.util.slack.SlackHelper import SlackHelper


class HashtagUsageResource(Resource):

    @staticmethod
    def post():
        date = datetime.combine(datetime.strptime(request.args.get('date'), '%Y-%m-%d').date(), datetime.min.time())
        HashtagUsageService.calculate_topics_hashtag_usage(date)

    @staticmethod
    def get():
        init = datetime.combine(datetime.strptime('2019-06-22', '%Y-%m-%d').date(), datetime.min.time())
        end = datetime.combine(datetime.strptime('2019-09-28', '%Y-%m-%d').date(), datetime.min.time())
        dates = [init + timedelta(days=i) for i in range((end - init).days + 1)]
        for date in dates:
            HashtagUsageService.calculate_topics_hashtag_usage(date)
        SlackHelper.post_message_to_channel(f'Finished hashtag and topic usage calculation.')
