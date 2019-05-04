from flask import make_response
from flask_restful import Resource


class RootResource(Resource):

    @staticmethod
    def get():
        return make_response("Ping", 200)
