from threading import Thread

from flask_restful import Resource

from src.util.CSVUtils import CSVUtils
from src.util.ResponseBuilder import ResponseBuilder


class CSVLoadingResource(Resource):

    @staticmethod
    def post():
        thread = Thread(target=CSVUtils.read_followers)
        thread.start()
        return ResponseBuilder.build('CSV Loading Started', 200)

    # @staticmethod
    # def put():
    #     CSVUtils.load_followers()
    #     return ResponseBuilder.build('CSV Loading Started', 200)
