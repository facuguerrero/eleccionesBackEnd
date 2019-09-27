from threading import Thread

from flask_restful import Resource

from src.service.user_network.UserNetworkAnalysisService import UserNetworkAnalysisService


class UserNetworkResource(Resource):

    @staticmethod
    def get():
        thread = Thread(target=UserNetworkAnalysisService.calculate_relationships)
        thread.start()
