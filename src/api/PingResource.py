from flask import make_response
from flask_restful import Resource
from src.util.scheduling.SchedulingExample import SchedulingExample


class PingResource(Resource):

    @staticmethod
    def get():
        return make_response(f'Ping. Count status: {SchedulingExample.COUNT}', 200)
