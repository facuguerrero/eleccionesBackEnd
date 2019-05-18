from threading import Thread

from flask import make_response
from flask_restful import Resource

from src.util.CSVUtils import CSVUtils


class CSVLoadingResource(Resource):

    @staticmethod
    def post():
        thread = Thread(target=CSVUtils.read_followers)
        thread.start()
        return make_response('CSV Loading Started', 200)
