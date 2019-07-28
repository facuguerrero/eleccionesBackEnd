from datetime import datetime

from flask import request
from flask_restful import Resource

from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.exception.WrongParametersError import WrongParametersError
from src.service.hashtags.HashtagUsageService import HashtagUsageService
from src.util.ResponseBuilder import ResponseBuilder


class HashtagUsageResource(Resource):

    @staticmethod
    def get(hashtag_name):
        # Parse input
        # TODO: Extract repeated date parsing logic
        try:
            start_date, end_date = HashtagUsageResource._check_query_params(request.args)
        except WrongParametersError as wpe:
            return ResponseBuilder.build_exception(wpe.message, 400)
        # Do function
        try:
            graph = HashtagUsageService().find_hashtag(hashtag_name, start_date, end_date)
            return ResponseBuilder.build(graph, 200)
        except NoDocumentsFoundError:
            return ResponseBuilder.build_exception('Requested data not found.', 404)

    @staticmethod
    def _check_query_params(query_params):
        """ Check expected query params and fail if compulsory fields are empty. """
        raw_start = query_params.get('start_date', None)
        start = HashtagUsageResource._parse_raw(raw_start, 'start_date')
        raw_end = query_params.get('end_date', None)
        end = HashtagUsageResource._parse_raw(raw_end, 'end_date', nullable=True)
        return start, end

    @staticmethod
    def _parse_raw(raw_date, id, nullable=False):
        # Throw exception if date can't be parsed or if it is None
        try:
            if raw_date is None:
                if nullable:
                    return None
                else:
                    raise ValueError()
            return datetime.strptime(raw_date, '%Y-%m-%d')
        except ValueError:
            raise WrongParametersError(id)
