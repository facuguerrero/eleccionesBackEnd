from datetime import timedelta, datetime

from src.db.dao.CommunityStrengthDAO import CommunityStrengthDAO
from src.db.dao.CooccurrenceGraphDAO import CooccurrenceGraphDAO
from src.db.dao.HashtagsTopicsDAO import HashtagsTopicsDAO
from src.db.dao.ShowableGraphDAO import ShowableGraphDAO
from src.service.hashtags.HashtagCooccurrenceService import HashtagCooccurrenceService
from src.service.hashtags.HashtagUsageService import HashtagUsageService
from src.service.hashtags.OSLOMService import OSLOMService
from src.service.topics.UserTopicService import UserTopicService
from src.util.DateUtils import DateUtils
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.graphs.GraphUtils import GraphUtils
from src.util.logging.Logger import Logger


class CooccurrenceAnalysisService:
    START_DAY = datetime.combine(datetime.strptime('2019-01-01', '%Y-%m-%d').date(), datetime.min.time())

    @classmethod
    def analyze(cls, last_day=None):
        """ Run cooccurrence analysis for the last day with all its intervals. """
        param_last_day = last_day
        # Run for previous day
        if not last_day:  # Parameter last_day should be the required day at 00:00:00
            last_day = datetime.combine((datetime.now() - timedelta(days=1)).date(), datetime.min.time())
        # Get last day at 23:59:59
        last_day = last_day + timedelta(days=1) - timedelta(seconds=1)  # This works because Python's sum is immutable
        # Run for last N days.
        for delta in ConfigurationManager().get_list('cooccurrence_deltas'):
            # Calculate start date from delta
            start_date = datetime.combine((last_day - timedelta(days=int(delta))).date(), datetime.min.time())
            # Run cooccurrence analysis
            cls.get_logger().info(f'Starting cooccurrence analysis for last {delta} days.')
            cls.analyze_cooccurrence_for_window(start_date, last_day)
            cls.get_logger().info(f'Cooccurrence analysis for last {delta} days done.')
        # Run usage analysis as soon as possible
        HashtagUsageService.calculate_topics_hashtag_usage(param_last_day)
        UserTopicService().init_process_with_date(DateUtils.today() if not param_last_day
                                                  else param_last_day + timedelta(days=1))

    @classmethod
    def analyze_cooccurrence_for_window(cls, start_date, end_date=None):
        """ Analyze cooccurrence for a given time window and generate cooccurrence graph. """
        end_date = cls.__validate_end_date(start_date, end_date)
        # Generate counting and id data
        HashtagCooccurrenceService.export_counts_for_time_window(start_date, end_date)
        # Run OSLOM and complete graph
        OSLOMService.export_communities_for_window(start_date, end_date)
        # Keep only needed data and unpack graph
        cls.get_logger().info(f'Generating cooccurrence graphs.')
        data = GraphUtils.create_cooccurrence_graphs(start_date, end_date)
        # Store result
        HashtagsTopicsDAO().store(data['hashtags_topics'], start_date, end_date)
        CommunityStrengthDAO().store(data['community_strength'], start_date, end_date)
        CooccurrenceGraphDAO().store(data['graphs'], start_date, end_date)
        ShowableGraphDAO().store(data['showable_graphs'], start_date, end_date)

    @classmethod
    def __validate_end_date(cls, start_date, end_date):
        # If there is only one day or both dates are the same, then we take from 00:00:00 to 23:59:59
        if end_date is None or start_date.date() == end_date.date():
            return start_date + timedelta(days=1) - timedelta(seconds=1)
        return end_date

    @classmethod
    def get_logger(cls):
        return Logger(cls.__name__)
