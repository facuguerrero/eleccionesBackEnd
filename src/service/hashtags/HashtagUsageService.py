from _operator import add
from datetime import timedelta, datetime

from src.db.dao.CooccurrenceDAO import CooccurrenceDAO
from src.db.dao.HashtagUsageDAO import HashtagUsageDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.ShowableGraphDAO import ShowableGraphDAO
from src.db.dao.TopicUsageDAO import TopicUsageDAO
from src.service.topics.UserTopicService import UserTopicService
from src.util.DateUtils import DateUtils
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger


class HashtagUsageService:
    """ Service designated to the calculation of the usage of hashtags in a given window of time. """
    # TODO: Move this to a single place (it is duplicated now)
    __parties = ['juntosporelcambio', 'frentedetodos', 'frentedespertar', 'consensofederal', 'frentedeizquierda']

    @classmethod
    def __validate_end_date(cls, start_date, end_date):
        # TODO: Extract this repeated code
        # If there is only one day or both dates are the same, then we take from 00:00:00 to 23:59:59
        if end_date is None or start_date.date() == end_date.date():
            return start_date + timedelta(days=1) - timedelta(seconds=1)
        return end_date

    @classmethod
    def calculate_topics_hashtag_usage(cls, end_date=None):
        """ Calculate the usage of all hashtags in today showable topics and the total usage of the topic itself. """
        supporters = cls.__generate_supporters_map()
        date = DateUtils.today() if not end_date else end_date + timedelta(days=1)
        # End time is yesterday at 23:59:59
        end_time = date - timedelta(seconds=1)
        # Run for different intervals of dates
        for delta in ConfigurationManager().get_list('showable_cooccurrence_deltas'):
            # Calculate start date from delta
            start_date = datetime.combine((end_time - timedelta(days=int(delta))).date(), datetime.min.time())
            # Calculate data
            cls.get_logger().info(f'Starting hashtag usage calculation for {delta} days window.')
            cls.calculate_hashtag_usage(start_date, end_time, interval='days', supporters=supporters)
            # Log finish for time checking
            cls.get_logger().info(f'Hashtag usage calculation finished for {delta} days window.')
        # Log finish for time checking
        cls.get_logger().info('Hashtag usage calculation finished.')
        # Once we've analyzed hashtags, topic usage calculations are just additions
        cls.get_logger().info('Starting topic usage calculation.')
        # Run for different intervals of dates
        for delta in ConfigurationManager().get_list('showable_cooccurrence_deltas'):
            # Calculate start date from delta
            start_date = datetime.combine((end_time - timedelta(days=int(delta))).date(), datetime.min.time())
            # Calculate data
            cls.get_logger().info(f'Starting topic usage calculation for {delta} days window.')
            cls.calculate_topic_usage(start_date, end_time, interval='days')
            # Log finish for time checking
            cls.get_logger().info(f'Topic usage calculation finished for {delta} days window.')
        # Log finish for time checking
        cls.get_logger().info('Topic usage calculation finished.')
        UserTopicService().init_update_support_follower()

    @classmethod
    def calculate_hashtag_usage(cls, start, end, interval, supporters):
        """ Calculate hashtag usage for given time window"""
        topics = ShowableGraphDAO().find_all(start, end)
        # Set used to calculate only once each hashtag
        processed_hashtags = set()
        # Run all topic analysis in parallel
        args = [[topic, start, end, interval, processed_hashtags, supporters] for topic in topics]
        AsyncThreadPoolExecutor().run_multiple_args(cls.__process_topic, args)

    @classmethod
    def calculate_topic_usage(cls, start, end, interval):
        """ Calculate the number of usages of all topics in the given interval of time. """
        topics = ShowableGraphDAO().find_all(start, end)
        # Run all topic analysis in parallel
        args = [[topic, start, end, interval] for topic in topics]
        AsyncThreadPoolExecutor().run_multiple_args(cls.__count_usages_for_topic, args)

    @classmethod
    def __process_topic(cls, topic, start, end, interval, processed_hashtags, supporters):
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
                # Create data structures
                date_axis = []
                count_axis = []
                parties_vectors = dict()
                supporters_count = dict()
                for party in cls.__parties:
                    parties_vectors[party] = []
                    supporters_count[party] = len(supporters[party])
                # Get hashtag usage for each date range
                for init, finish in dates:
                    # Get a list of all the different users that used the hashtag for the given time window
                    users = set(CooccurrenceDAO().distinct_users(hashtag, init, finish))
                    count = len(users)
                    date_axis.append(init)
                    count_axis.append(count)
                    # Calculate the proportion of usage for each party
                    party_counts = []
                    for party in cls.__parties:
                        # Get the proportion of users of each party that used the given hashtag
                        party_counts.append(len(users.intersection(supporters[party]))/supporters_count[party])
                    # Append results to each party's vector
                    for party, i in zip(cls.__parties, range(len(cls.__parties))):
                        parties_vectors[party].append(party_counts[i])
                # Store data needed for line plotting
                HashtagUsageDAO().store(hashtag, start, end, date_axis, count_axis, parties_vectors)
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
        # Create dictionary to keep usage proportions
        parties_proportions = {party: [0]*len(date_axis) for party in cls.__parties}
        # Iterate through all topic's hashtags and add their counts to the axis
        for hashtag in hashtags:
            document = HashtagUsageDAO().find(hashtag, start, end)
            hashtag_count_axis = document['count_axis']
            count_axis = list(map(add, count_axis, hashtag_count_axis))
            # Calculate the usage proportion of each party
            parties_vectors = document['parties_vectors']
            for party, vector in parties_vectors.items():
                # Add all hashtags' usage proportion vectors for each party
                parties_proportions[party] = [sum(x) for x in zip(vector, parties_proportions[party])]
        # Store topic usage data
        TopicUsageDAO().store(topic['topic_id'], start, end, date_axis, count_axis, parties_proportions)

    @classmethod
    def __generate_dates_in_interval(cls, start, end, interval):
        """ Returns a list of tuples with start and end dates for a given interval. """
        diff = 24 if interval == 'hours' else (end - start).days
        return [(start + timedelta(**{interval: i}), start + timedelta(**{interval: i + 1})) for i in range(diff)]

    @classmethod
    def __generate_supporters_map(cls):
        """ Creates a map which relates each party with a set of its followers. """
        supporters = dict()
        for party in cls.__parties:
            users = [follower['_id'] for follower in RawFollowerDAO().get_all({
                '$and': [{'probability_vector_support': {'$elemMatch': {'$gte': 0.8}}}, {'support': party}]
            })]
            supporters[party] = users
        return supporters

    @classmethod
    def get_logger(cls):
        return Logger(cls.__name__)
