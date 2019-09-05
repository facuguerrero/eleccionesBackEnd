from datetime import datetime, timedelta
from flask_restful import Resource

from src.service.hashtags.CooccurrenceAnalysisService import CooccurrenceAnalysisService
from src.util.slack.SlackHelper import SlackHelper


class CooccurrenceAnalysisResource(Resource):
    # TODO: Remove this endpoint and replace for scheduled task

    @staticmethod
    def post():
        # TODO: Remove this!
        init = datetime.combine(datetime.strptime('2019-06-22', '%Y-%m-%d').date(), datetime.min.time())
        end = datetime.combine(datetime.strptime('2019-08-27', '%Y-%m-%d').date(), datetime.min.time())
        dates = [init + timedelta(days=i) for i in range((end-init).days + 1)]
        for date in dates:
            CooccurrenceAnalysisService.analyze(last_day=date)
            SlackHelper.post_message_to_channel(f'Finished cooccurrence graph generation for date {date}.')
