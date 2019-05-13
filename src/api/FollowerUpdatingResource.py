from threading import Thread

from flask import make_response
from flask_restful import Resource

from src.service.followers.FollowerUpdateService import FollowerUpdateService


class FollowerUpdatingResource(Resource):

    @staticmethod
    def get():
        thread = Thread(target=FollowerUpdateService.update_followers)
        thread.start()
        return make_response('Follower Updating Started', 200)
