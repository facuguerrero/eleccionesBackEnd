from datetime import timedelta, datetime

from src.db.dao.CooccurrenceGraphDAO import CooccurrenceGraphDAO
from src.service.hashtags.HashtagCooccurrenceService import HashtagCooccurrenceService
from src.service.hashtags.OSLOMService import OSLOMService
from src.util.graphs.GraphUtils import GraphUtils
from src.util.logging.Logger import Logger


class CooccurrenceAnalysisService:

    START_DAY = datetime.strptime('2019-01-01', '%Y-%m-%d')

    @classmethod
    def run_analysis(cls):
        """ Run cooccurrence analysis for the last day and the accumulated since 2019-01-01. """
        last_day = datetime.now() - timedelta(days=1)
        # Run for previous day
        cls.get_logger().info(f'Starting cooccurrence analysis for single day {last_day.date()}.')
        cls.analyze_cooccurrence_for_window(last_day)
        cls.get_logger().info('Daily cooccurrence analysis done.')
        # Run accumulated
        cls.get_logger().info(f'Starting cooccurrence analysis for full period from first day until yesterday.')
        cls.analyze_cooccurrence_for_window(last_day, cls.START_DAY)
        cls.get_logger().info(f'Accumulated cooccurrence analysis done.')

    @classmethod
    def analyze_cooccurrence_for_window(cls, start_date, end_date=None):
        """ Analyze cooccurrence for a given time window and generate cooccurrence graph. """
        end_date = cls.__validate_end_date(start_date, end_date)
        # Generate counting and id data
        counts = HashtagCooccurrenceService.export_counts_for_time_window(start_date, end_date)
        # Create graph
        graph = GraphUtils.create_with_weighted_edges(counts)
        # Run OSLOM and complete graph
        OSLOMService.export_communities_for_window(start_date, end_date, graph)
        # Keep only needed data and unpack graph
        unpacked = GraphUtils.unpack_nodes(graph)
        # Store result
        CooccurrenceGraphDAO().store(unpacked, start_date, end_date)

    @classmethod
    def get_graph_for_window(cls, start_date, end_date):
        """ Returns, if existent, the cooccurrence graph that belongs to the given date window. """
        end_date = cls.__validate_end_date(start_date, end_date)
        return CooccurrenceGraphDAO().find(start_date, end_date)

    @classmethod
    def __validate_end_date(cls, start_date, end_date):
        # If there is only one day or both dates are the same, then we take from 00:00:00 to 23:59:59
        if end_date is None or start_date.date() == end_date.date():
            return start_date + timedelta(days=1) - timedelta(seconds=1)
        return end_date

    @classmethod
    def get_logger(cls):
        return Logger(cls.__name__)