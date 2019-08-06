from threading import Thread

from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.util.logging.Logger import Logger
from src.util.slack.SlackHelper import SlackHelper


class FollowerSupportService:
    FACTOR = 0.8

    @classmethod
    def init_update_support_follower(cls):
        thread = Thread(target=FollowerSupportService.init_process)
        thread.start()

    @classmethod
    def init_process(cls):
        try:
            cls.update_support_follower()
        except Exception as e:
            cls.get_logger().error("FollowerSupport updating failed.")
            cls.get_logger().error(e)
            SlackHelper().post_message_to_channel('Fallo el update de follower support.', '#errors')

    @classmethod
    def update_support_follower(cls):
        """ Method for updating follower support's vector. """
        cls.get_logger().info("Starting FollowerSupport updating.")
        rt_vectors, candidate_index, groups_quantity, candidate_group = cls.get_users_rt_vector()

        # Get followers which have tweets
        followers_with_tweets = RawFollowerDAO().get_all({'has_tweets': True})
        cls.get_logger().info("Calculating probability vector support")
        for follower in followers_with_tweets:
            user_id = follower['_id']
            rt_vector = rt_vectors.get(user_id, [0] * groups_quantity)
            follows_vector = cls.get_follows_vector(follower, candidate_index, groups_quantity)

            final_rt, final_follows = cls.get_final_vectors(rt_vector, follows_vector)
            # Calculate probability vector and save it
            probability_vector = [sum(x) for x in zip(final_rt, final_follows)]
            cls.save_follower_vectors(user_id, probability_vector, rt_vector, candidate_group)
        cls.get_logger().info("Finishing FollowerSupport updating.")

    @classmethod
    def get_users_rt_vector(cls):
        """ Get data from db and create users_rt_vectors. """
        # {candidate: index}, [candidate_id]
        candidate_index, candidates_list, candidate_group, candidates_rt_cursor = cls.get_necessary_data()
        cls.get_logger().info("Candidates and theirs rt are retrieved correctly. ")
        groups_quantity = max(candidate_index.values()) + 1
        rt_vectors = {}
        for tweet in candidates_rt_cursor:
            # Get user information
            user = tweet['user_id']
            user_rt_vector = cls.get_user_vector_or_default(user, groups_quantity, rt_vectors)

            # If tweet creator is a candidate, plus one in user's vector
            user_tweet_creator = tweet['retweeted_status']['user']['screen_name']
            if user_tweet_creator in candidates_list:
                user_rt_vector[candidate_index[user_tweet_creator]] += 1

            if sum(user_rt_vector) > 0:
                rt_vectors[user] = user_rt_vector

        cls.get_logger().info("RT vectors are created correctly. ")
        return rt_vectors, candidate_index, groups_quantity, candidate_group

    @classmethod
    def get_necessary_data(cls):
        """ Retrieve db data and create candidates list. """
        candidate_index, candidate_group = CandidateDAO().get_required_candidates()
        candidates_list = list(candidate_index.keys())
        candidates_rt_cursor = RawTweetDAO().get_rt_to_candidates_cursor(candidates_list)
        return candidate_index, candidates_list, candidate_group, candidates_rt_cursor

    @classmethod
    def get_user_vector_or_default(cls, user, candidates_quantity, rt_vectors):
        return rt_vectors[user] if user in rt_vectors else [0] * candidates_quantity

    @classmethod
    def get_follows_vector(cls, follower, candidate_index, groups_quantity):
        follows_vector = follower.get('follows', [])
        vector_to_return = [0] * groups_quantity
        for candidate in follows_vector:
            # Don't count candidates that are not important
            if candidate in candidate_index:
                vector_to_return[candidate_index[candidate]] += 1
        return vector_to_return

    @classmethod
    def multiply_by_factor(cls, vector, factor, normalization):
        """Multiplies all elements by given factor"""
        return vector if normalization == 0 else map(lambda x: x * (factor / normalization), vector)

    @classmethod
    def get_final_vectors(cls, rt_vector, follows_vector):
        """ If two vectors have elements, then multiply all elements by factor / total_elements for normalization.
        Else, return vectors without any modification. """
        total_rt = sum(rt_vector)
        total_follows = sum(follows_vector)
        if total_rt > 0 and total_follows > 0:
            return cls.multiply_by_factor(rt_vector, FollowerSupportService.FACTOR, total_rt), \
                   cls.multiply_by_factor(follows_vector, 1.0 - FollowerSupportService.FACTOR, total_follows)
        return rt_vector, follows_vector

    @classmethod
    def save_follower_vectors(cls, user, probability_vector, rt_vector, candidate_group):
        """ If follower have non zero vectors, save them"""
        data_to_save = {}

        # Adds probability vector
        if sum(probability_vector) > 0:
            data_to_save['probability_vector_support'] = probability_vector

        # Adds retweets vector
        if sum(rt_vector) > 0:
            data_to_save['rt_vector'] = rt_vector

        # If has one probability greather than 0.5, adds support
        max_probability = max(probability_vector)
        if max_probability > 0.5:
            candidate_index = probability_vector.index(max_probability)
            data_to_save['support'] = candidate_group[candidate_index]

        if len(data_to_save) != 0:
            cls.update_followers_vector(user, data_to_save)

    @classmethod
    def update_followers_vector(cls, user, data):
        """ For every user, update their rt_vector. """
        RawFollowerDAO().update_first({'_id': user}, data)

    @classmethod
    def get_logger(cls):
        return Logger('FollowerSupportService')
