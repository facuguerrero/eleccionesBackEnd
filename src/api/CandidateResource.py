from flask import request
from flask_restful import Resource

from src.db.dao.CandidatesFollowersDAO import CandidatesFollowersDAO
from src.exception.CandidateAlreadyExistsError import CandidateAlreadyExistsError
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.service.candidates.CandidateService import CandidateService
from src.util.ResponseBuilder import ResponseBuilder


class CandidateResource(Resource):

    @staticmethod
    def post():
        query_params = request.args
        screen_name = query_params.get('screen_name', None)
        nickname = query_params.get('nickname', None)
        try:
            CandidateService().add_candidate(screen_name, nickname)
            return ResponseBuilder.build(f'Candidate {screen_name} successfully added.', 200)
        except CandidateAlreadyExistsError as caee:
            return ResponseBuilder.build_exception(caee.message, 400)

    @staticmethod
    def get(screen_name=None):
        # Return all candidates
        if not screen_name:
            return ResponseBuilder.build(CandidatesFollowersDAO().get_all_increases(), 200)
        # Return only the candidate that matches the given screen name
        try:
            return ResponseBuilder.build(CandidatesFollowersDAO().get_increases_for_candidate(screen_name), 200)
        except NoDocumentsFoundError as ndfe:
            return ResponseBuilder.build_exception(ndfe.message, 400)
