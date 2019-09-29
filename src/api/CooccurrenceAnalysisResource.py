from datetime import datetime, timedelta

from flask import request
from flask_restful import Resource

from src.service.hashtags.CooccurrenceAnalysisService import CooccurrenceAnalysisService
from src.util.slack.SlackHelper import SlackHelper


class CooccurrenceAnalysisResource(Resource):

    @staticmethod
    def post():
        date = datetime.combine(datetime.strptime(request.args.get('date'), '%Y-%m-%d').date(), datetime.min.time())
        CooccurrenceAnalysisService.analyze(last_day=date)
        SlackHelper.post_message_to_channel(f'Finished cooccurrence graph generation for date {date}.')
        # init = datetime.combine(datetime.strptime('2019-08-04', '%Y-%m-%d').date(), datetime.min.time())
        # end = datetime.combine(datetime.strptime('2019-09-29', '%Y-%m-%d').date(), datetime.min.time())
        # dates = [init + timedelta(days=i) for i in range((end - init).days + 1)]
        # for date in dates:
        #     CooccurrenceAnalysisService.analyze(last_day=date)
        #     SlackHelper.post_message_to_channel(f'Finished cooccurrence graph generation for date {date}.')
