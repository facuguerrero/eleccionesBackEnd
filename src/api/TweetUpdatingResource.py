from threading import Thread

from flask import make_response
from flask_restful import Resource

from src.service.tweets.TweetUpdateService import TweetUpdateService


class TweetUpdatingResource(Resource):
    """ This endpoint exists to force tweet updating. """

    @staticmethod
    def post():
        thread = Thread(target=TweetUpdateService.update_tweets)
        thread.start()
        return make_response('Tweets Updating Started', 200)