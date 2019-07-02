from threading import Thread

from flask import make_response
from flask_restful import Resource

from src.service.tweets.TweetUpdateServiceInitializer import TweetUpdateServiceInitializer


class TweetUpdatingResource(Resource):
    """ This endpoint exists to force tweet updating. """

    @staticmethod
    def post():
        thread = Thread(target=TweetUpdateServiceInitializer.initialize_tweet_update_service)
        thread.start()
        return make_response('Tweets Updating Started', 200)
