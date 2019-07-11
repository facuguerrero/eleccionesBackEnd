from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.util.logging.Logger import Logger


class FollowerSupportService:

    @classmethod
    def update_follower_support(cls):
        """ Method for updating follower support's vector. """
        cls.get_logger().info("Updating follower's support vector.")
        rt_vectors = cls.get_users_rt_vector()
        cls.update_followers_vector(rt_vectors)
        # TODO get follows vector and update support_vector

    @classmethod
    def get_users_rt_vector(cls):
        """ Get data from db and create users_rt_vectors. """
        # {candidate: index}, [candidate_id]
        candidate_index, candidates_list, candidates_rt_cursor = cls.get_necessary_data()
        cls.get_logger().info("Candidates and theirs rt are retrieved correctly. ")
        candidates_quantity = int(len(candidates_list) / 2)
        rt_vectors = {}
        for tweet in candidates_rt_cursor:
            # Get user information
            user = tweet['user_id']
            user_rt_vector = cls.get_user_vector_or_default(user, candidates_quantity, rt_vectors)

            # If tweet creator is a candidate, plus one in user's vector
            user_tweet_creator = tweet['retweeted_status']['user']['screen_name']
            if user_tweet_creator in candidates_list:
                user_rt_vector[candidate_index[user_tweet_creator]] += 1

            if sum(user_rt_vector) > 0:
                rt_vectors[user] = user_rt_vector
        return rt_vectors

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
    def update_followers_vector(cls, rt_vectors):
        """ For every user, update ther rt_vector. """
        for user, vector in rt_vectors:
            RawFollowerDAO().update_first(
                {'_id': user},
                {'$set':
                     {'rt_vector': vector}
                 }
            )
        cls.get_logger().info("Follower's rt vector updated correctly.")

    @classmethod
    def get_logger(cls):
        return Logger('FollowerSupportService')
