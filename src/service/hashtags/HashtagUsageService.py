from _operator import add
from datetime import timedelta, datetime

from src.db.dao.CooccurrenceDAO import CooccurrenceDAO
from src.db.dao.HashtagUsageDAO import HashtagUsageDAO
from src.db.dao.ShowableGraphDAO import ShowableGraphDAO
from src.db.dao.TopicUsageDAO import TopicUsageDAO
from src.mapper.response.HashtagUsageResponseMapper import HashtagUsageResponseMapper
from src.util.DateUtils import DateUtils
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils
from src.util.logging.Logger import Logger


class HashtagUsageService:
    """ Service designated to the calculation of the usage of hashtags in a given window of time. """
    # TODO: Move this to a single place (it is duplicated now)
    START_DAY = datetime.combine(datetime.strptime('2019-01-01', '%Y-%m-%d').date(), datetime.min.time())

    @classmethod
    def find_hashtag(cls, hashtag_name, start_date, end_date):
        end_date = cls.__validate_end_date(start_date, end_date)
        document = HashtagUsageDAO().find(hashtag_name, start_date, end_date)
        return HashtagUsageResponseMapper.map_one(document)

    @classmethod
    def find_topic(cls, topic_id, start_date, end_date):
        end_date = cls.__validate_end_date(start_date, end_date)
        document = TopicUsageDAO().find(topic_id, start_date, end_date)
        return HashtagUsageResponseMapper.map_one(document)

    @classmethod
    def __validate_end_date(cls, start_date, end_date):
        # TODO: Extract this repeated code
        # If there is only one day or both dates are the same, then we take from 00:00:00 to 23:59:59
        if end_date is None or start_date.date() == end_date.date():
            return start_date + timedelta(days=1) - timedelta(seconds=1)
        return end_date

    @classmethod
    def calculate_today_topics_hashtag_usage(cls):
        """ Calculate the usage of all hashtags in today showable topics and the total usage of the topic itself. """
        # End time is yesterday at 23:59:59
        end_time = DateUtils.today() - timedelta(seconds=1)
        # Start time is yesterday at 00:00:00
        start_time = DateUtils.today() - timedelta(days=1)
        cls.get_logger().info('Starting daily hashtag usage calculation.')
        # Calculate daily data
        cls.calculate_hashtag_usage(start_time, end_time, interval='hours')
        # Calculate accumulated data
        cls.get_logger().info('Starting accumulated hashtag usage calculation.')
        cls.calculate_hashtag_usage(cls.START_DAY, end_time, interval='days')
        # Log finish for time checking
        cls.get_logger().info('Hashtag usage calculation finished.')
        # Once we've analyzed hashtags, topic usage calculations are just additions
        cls.get_logger().info('Starting daily topic usage calculation.')
        # Daily
        cls.calculate_topic_usage(start_time, end_time, interval='hours')
        cls.get_logger().info('Starting accumulated topic usage calculation.')
        # Accumulated
        cls.calculate_topic_usage(cls.START_DAY, end_time, interval='days')
        # Log finish for time checking
        cls.get_logger().info('Topic usage calculation finished.')

    @classmethod
    def calculate_hashtag_usage(cls, start, end, interval):
        """ Calculate hashtag usage for given time window"""
        topics = ShowableGraphDAO().find_all(start, end)
        # Set used to calculate only once each hashtag
        processed_hashtags = set()
        # Run all topic analysis in parallel
        args = [[topic, start, end, interval, processed_hashtags] for topic in topics]
        AsyncThreadPoolExecutor().run_multiple_args(cls.__process_topic, args)

    @classmethod
    def calculate_topic_usage(cls, start, end, interval):
        """ Calculate the number of usages of all topics in the given interval of time. """
        topics = ShowableGraphDAO().find_all(start, end)
        # Run all topic analysis in parallel
        args = [[topic, start, end, interval] for topic in topics]
        AsyncThreadPoolExecutor().run_multiple_args(cls.__count_usages_for_topic, args)

    @classmethod
    def __process_topic(cls, topic, start, end, interval, processed_hashtags):
        """ Get all the hashtags used in the current topic """
        # Do not process the 'topic of topics'
        if topic['topic_id'] == 'main': return
        # Get all topic's hashtags
        hashtags = [node['id'] for node in topic['graph']['nodes']]
        for hashtag in hashtags:
            # Create a lock for the processing of this specific hashtag
            lock_name = f'{hashtag}-usage'
            ConcurrencyUtils().create_lock(lock_name)
            # If it is already being processed, then do nothing
            if not ConcurrencyUtils().acquire_lock(lock_name, block=False): continue
            try:
                # If the hashtag was already processed, do nothing
                if hashtag in processed_hashtags: continue
                # Create an interval for each hour of the day or day of the interval
                dates = cls.__generate_dates_in_interval(start, end, interval)
                # Get hashtag usage for each hour
                date_axis = []
                count_axis = []
                for init, finish in dates:
                    # Get a list of all the different users that used the hashtag for the given time window
                    count = len(CooccurrenceDAO().distinct_users(hashtag, init, finish))
                    date_axis.append(init)
                    count_axis.append(count)
                # Store data needed for line plotting
                HashtagUsageDAO().store(hashtag, start, end, date_axis, count_axis)
                # Mark as processed
                processed_hashtags.add(hashtag)
            finally:
                # Always free release the lock
                ConcurrencyUtils().release_lock(lock_name)

    @classmethod
    def __count_usages_for_topic(cls, topic, start, end, interval):
        """ Counts the number of usages of the full topic. The result is the sum of the usages of all its hashtags. """
        # Do not process the 'topic of topics'
        if topic['topic_id'] == 'main': return
        # Get all topic's hashtags
        hashtags = [node['id'] for node in topic['graph']['nodes']]
        # Create date axis data
        date_axis = [init for init, finish in cls.__generate_dates_in_interval(start, end, interval)]
        # Create count axis array to do additions easier
        count_axis = [0] * len(date_axis)
        # Iterate through all topic's hashtags and add their counts to the axis
        for hashtag in hashtags:
            hashtag_count_axis = HashtagUsageDAO().find(hashtag, start, end)['count_axis']
            count_axis = list(map(add, count_axis, hashtag_count_axis))
        # Store topic usage data
        TopicUsageDAO().store(topic['topic_id'], start, end, date_axis, count_axis)

    @classmethod
    def __generate_dates_in_interval(cls, start, end, interval):
        """ Returns a list of tuples with start and end dates for a given interval. """
        diff = 24 if interval == 'hours' else (end - start).days
        return [(start + timedelta(**{interval: i}), start + timedelta(**{interval: i + 1})) for i in range(diff)]

    @classmethod
    def get_logger(cls):
        return Logger(cls.__name__)
