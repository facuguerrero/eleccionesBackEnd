from flask import request, make_response
from flask_restful import Resource

from src.exception.CandidateAlreadyExistsError import CandidateAlreadyExistsError
from src.service.candidates.CandidateService import CandidateService


class CandidateResource(Resource):

    @staticmethod
    def post():
        query_params = request.args
        screen_name = query_params.get('screen_name', None)
        nickname = query_params.get('nickname', None)
        try:
            CandidateService().add_candidate(screen_name, nickname)
            return make_response(f'Candidate {screen_name} successfully added.', 200)
        except CandidateAlreadyExistsError as caee:
            return make_response(caee.message, 400)
