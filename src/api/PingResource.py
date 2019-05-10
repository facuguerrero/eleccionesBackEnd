from flask import make_response
from flask_restful import Resource


class PingResource(Resource):

    @staticmethod
    def get():
        return make_response(f'Ping.', 200)
