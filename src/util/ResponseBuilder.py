from json import dumps

from flask import make_response


class ResponseBuilder:

    @staticmethod
    def build(body, status):
        return make_response(dumps(body), status)

    @staticmethod
    def build_exception(message, status):
        body = dumps({'message': message, 'status': status})
        return make_response(body, status)
