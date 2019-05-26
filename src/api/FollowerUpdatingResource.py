from threading import Thread

from flask_restful import Resource

from src.service.followers.FollowerUpdateService import FollowerUpdateService
from src.util.ResponseBuilder import ResponseBuilder


class FollowerUpdatingResource(Resource):
    """ This endpoint exists only to force updating. """

    @staticmethod
    def patch():
        thread = Thread(target=FollowerUpdateService.update_followers)
        thread.start()
        return ResponseBuilder.build('Follower Updating Started', 200)
