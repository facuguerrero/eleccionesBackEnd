from flask_restful import Resource
from src.util.PreProcessingTweetsUtil import PreProcessingTweetsUtil
from src.util.ResponseBuilder import ResponseBuilder


class PreProcessingTweetsResource(Resource):

    @staticmethod
    def post():
        PreProcessingTweetsUtil().load_tweets()
        return ResponseBuilder.build('Tweets loaded successfully', 200)

    @staticmethod
    def put():
        PreProcessingTweetsUtil().load_followers()
        return ResponseBuilder.build('Followers loaded successfully', 200)
