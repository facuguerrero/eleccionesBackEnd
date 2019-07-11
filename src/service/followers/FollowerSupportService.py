from threading import Thread

from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.util.logging.Logger import Logger


class FollowerSupportService:
    FACTOR = 0.8

    @classmethod
    def init_update_support_follower(cls):
        thread = Thread(target=FollowerSupportService.update_follower_support)
        thread.start()

    @classmethod
    def update_follower_support(cls):
        """ Method for updating follower support's vector. """
        cls.get_logger().info("Starting FollowerSupport updating.")
        rt_vectors, candidate_index, groups_quantity = cls.get_users_rt_vector()

        # Get followers which have tweets
        followers_with_tweets = RawFollowerDAO().get_all({'has_tweets': True})
        for follower in followers_with_tweets:
            user_id = follower['_id']
            rt_vector = rt_vectors.get(user_id, [0] * groups_quantity)
            follows_vector = cls.get_follows_vector(follower, candidate_index, groups_quantity)

            # Multiply all elements by factor / total_elements for normalization.
            final_rt = cls.multiply_by_factor(rt_vector, FollowerSupportService.FACTOR, sum(rt_vector))
            final_follows = cls.multiply_by_factor(follows_vector, 1.0 - FollowerSupportService.FACTOR,
                                                   sum(follows_vector))

            # Calculate probability vector and save it
            probability_vector = [sum(x) for x in zip(final_rt, final_follows)]
            cls.save_follower_vectors(user_id, probability_vector, rt_vector)
        cls.get_logger().info("Finishing FollowerSupport updating.")


    @classmethod
    def get_users_rt_vector(cls):
        """ Get data from db and create users_rt_vectors. """
        # {candidate: index}, [candidate_id]
        candidate_index, candidates_list, candidates_rt_cursor = cls.get_necessary_data()
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
        return rt_vectors, candidate_index, groups_quantity

    @classmethod
    def get_necessary_data(cls):
        """ Retrieve db fata and create candidates list. """
        candidate_index = CandidateDAO().get_required_candidates()
        candidates_list = list(candidate_index.keys())
        candidates_rt_cursor = RawTweetDAO().get_rt_to_candidates_cursor(candidates_list)
        return candidate_index, candidates_list, candidates_rt_cursor

    @classmethod
    def get_user_vector_or_default(cls, user, candidates_quantity, rt_vectors):
        return rt_vectors[user] if user in rt_vectors else [0] * candidates_quantity

    @classmethod
    def get_follows_vector(cls, follower, candidate_index, groups_quantity):
        follows_vector = follower.get('follows', [])
        vector_to_return = [0] * groups_quantity
        for candidate in follows_vector:
            vector_to_return[candidate_index[candidate]] += 1
        return vector_to_return

    @classmethod
    def multiply_by_factor(cls, vector, factor, normalization):
        """Multiplies all elements by given factor"""
        return vector if normalization == 0 else map(lambda x: x * (factor / normalization), vector)

    @classmethod
    def save_follower_vectors(cls, user, probability_vector, rt_vector):
        """ If follower have non zero vectors, save them"""
        data_to_save = {}
        if sum(probability_vector) > 0:
            data_to_save['probability_vector_support'] = probability_vector
        if sum(rt_vector) > 0:
            data_to_save['rt_vector'] = rt_vector
        if len(data_to_save) != 0:
            cls.update_followers_vector(user, data_to_save)

    @classmethod
    def update_followers_vector(cls, user, data):
        """ For every user, update their rt_vector. """
        RawFollowerDAO().update_first(
            {'_id': user},
            {'$set':
                 data
             }
        )

    @classmethod
    def get_logger(cls):
        return Logger('FollowerSupportService')
