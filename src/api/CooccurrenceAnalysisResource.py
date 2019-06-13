from datetime import datetime, timedelta
from flask import request
from flask_restful import Resource

from src.exception.NoHashtagCooccurrenceError import NoHashtagCooccurrenceError
from src.exception.WrongParametersError import WrongParametersError
from src.service.hashtags.HashtagCooccurrenceService import HashtagCooccurrenceService
from src.service.hashtags.OSLOMService import OSLOMService
from src.util.ResponseBuilder import ResponseBuilder


class CooccurrenceAnalysisResource(Resource):

    @staticmethod
    def get():
        # Parse input
        try:
            start_date, end_date = CooccurrenceAnalysisResource._check_query_params(request.args)
        except WrongParametersError as wpe:
            return ResponseBuilder.build_exception(wpe.message, 400)
        # Do function
        try:
            # If there is only one day or both dates are the same, then we take from 00:00 to 23:59
            if end_date is None or start_date.date() == end_date.date():
                end_date = start_date + timedelta(days=1) - timedelta(seconds=1)
            HashtagCooccurrenceService().export_counts_for_time_window(start_date, end_date)
            OSLOMService().export_communities_for_window(start_date, end_date)
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
        return start, end

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
