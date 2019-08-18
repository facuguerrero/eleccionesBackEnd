from datetime import datetime
from flask import request
from flask_restful import Resource

from src.exception.NoHashtagCooccurrenceError import NoHashtagCooccurrenceError
from src.exception.WrongParametersError import WrongParametersError
from src.service.hashtags.CooccurrenceAnalysisService import CooccurrenceAnalysisService
from src.util.ResponseBuilder import ResponseBuilder


class CooccurrenceAnalysisResource(Resource):
    # TODO: Remove this endpoint and replace for scheduled task

    @staticmethod
    def get():
        # Parse input
        try:
            start_date, end_date, cutting_method = CooccurrenceAnalysisResource._check_query_params(request.args)
        except WrongParametersError as wpe:
            return ResponseBuilder.build_exception(wpe.message, 400)
        # Do function
        try:
            CooccurrenceAnalysisService.analyze_cooccurrence_for_window(start_date, end_date, cutting_method)
            return ResponseBuilder.build('Analysis .txt and .csv files were created.', 200)
        except NoHashtagCooccurrenceError as nhce:
            return ResponseBuilder.build_exception(nhce.message, 400)

    @staticmethod
    def _check_query_params(query_params):
        """ Check expected query params and fail if compulsory fields are empty. """
        raw_start = query_params.get('start_date', None)
        start = CooccurrenceAnalysisResource._parse_raw(raw_start, 'start_date')
        raw_end = query_params.get('end_date', None)
        end = CooccurrenceAnalysisResource._parse_raw(raw_end, 'end_date', nullable=True)
        cutting_method = query_params.get('cutting_method', None)
        return start, end, cutting_method

    @staticmethod
    def _parse_raw(raw_date, id, nullable=False):
        # Throw exception if date can't be parsed or if it is None
        try:
            if raw_date is None:
                if nullable: return None
                else: raise ValueError()
            return datetime.strptime(raw_date, '%Y-%m-%d')
        except ValueError:
            raise WrongParametersError(id)
