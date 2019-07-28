from flask_restful import Resource

from src.util.ResponseBuilder import ResponseBuilder


class PingResource(Resource):

    @staticmethod
    def get():
        return ResponseBuilder.build(f'Ping.', 200)
