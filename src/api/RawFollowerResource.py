from flask import request
from flask_restful import Resource

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.util.ResponseBuilder import ResponseBuilder


class RawFollowerResource(Resource):

    @staticmethod
    def get(candidate_name=None):
        query_params = request.args
        start = int(query_params.get('start', 0))
        limit = int(query_params.get('limit', 5000))
        # Return all followers
        if not candidate_name:
            response = RawFollowerDAO().get_all_with_cursor(start, limit)
        else:
            # Return only those followers who follow the given user
            try:
                response = RawFollowerDAO().get_following_with_cursor(candidate_name, start, limit)
            except NoDocumentsFoundError as ndfe:
                return ResponseBuilder.build_exception(ndfe.message, 400)
        return ResponseBuilder.build(response, 200)
