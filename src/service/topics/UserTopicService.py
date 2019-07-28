import datetime
from threading import Thread

import pandas as pd
from scipy.sparse import csr_matrix, save_npz
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.preprocessing import normalize

from src.db.dao.CooccurrenceGraphDAO import CooccurrenceGraphDAO
from src.db.dao.HashtagsTopicsDAO import HashtagsTopicsDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO
from src.util.logging.Logger import Logger


class UserTopicService:
    # TODO fle path
    # FILE_PATH = f"{abspath(join(dirname(__file__), '../../'))}/resources/candidates.json"

    @classmethod
    def init_update_support_follower(cls):
        thread = Thread(target=UserTopicService.init_process)
        thread.start()

    @classmethod
    def init_process(cls):
        try:
            cls.get_logger().info("Calculating User-Topic Matrix")
            cls.calculate_users_topics_matrix()
        except Exception as e:
            cls.get_logger().error("Error Calculating User-Topic Matrix")
            cls.get_logger().error(e)
            # SlackHelper().post_message_to_channel('Fallo el update de follower support.', '#errors')

    @classmethod
    def calculate_users_topics_matrix(cls):
        """ This method calculate the user-topic matrix. """

        # Retrieve necessaries data
        users_hashtags_matrix, hashtags_topics_matrix, users_index = cls.get_necessary_data()
        cls.get_logger().info("Data retrieved correctly. ")
        # Multiply this matrix and get users_topics matrix
        users_topics_matrix = users_hashtags_matrix.multiply(hashtags_topics_matrix)
        cls.get_logger().info("Users_topics matrix created correctly.")

        # Apply TF-IDF
        tfidf_transformer = TfidfTransformer()
        tf_idf_matrix = tfidf_transformer.fit_transform(users_topics_matrix)
        # Save matrix
        cls.save_matrix(tf_idf_matrix)
        cls.get_logger().info("Finished process ")

        # Retrieve users grouped
        # grouped_users = cls.get_grouped_users(users_index)
        # for group, users_list in grouped_users.items():
        # group_vector = cls.get_matrix_from_data(users_list)
        # group_matrix = cls.get_group_matrix(tf_idf_matrix, group_vector)

    @classmethod
    def get_necessary_data(cls):
        """ Retrieve from database all necessary data.
        user_hashtag matrix normalized
        hashtag_topic matrix
        user_index are the user's row position in user_hashtag_matrix
        """

        # Retrieve last 3 days hashtags list sorted alphabetically
        last_3_days_hashtags = UserHashtagDAO().get_last_3_days_hashtags()
        cls.get_logger().info("All hashtags from 3 days ago are retrieved")

        # Get hashtags-topics matrix
        all_topics_sorted = CooccurrenceGraphDAO().get_all_sorted_topics()
        hashtags_topics_data = HashtagsTopicsDAO().get_required_hashtags(last_3_days_hashtags, all_topics_sorted)
        cls.get_logger().info("Hashtags topics retrieved")
        hashtags_topics_matrix = cls.get_matrix_from_data(hashtags_topics_data)

        # Get users-hashtags data and users index structure {user: matrix_index}
        # Users_index are the user's row in matrix
        users_hashtags_data, users_index = UserHashtagDAO().get_last_3_days_users_and_hashtags(last_3_days_hashtags)
        cls.get_logger().info("All users-hashtags from 3 days ago are retrieved")
        users_hashtags_matrix = cls.get_matrix_from_data(users_hashtags_data)
        # Axis 1 normalize by row
        normalized_user_hashtag_matrix = normalize(users_hashtags_matrix, norm='l1', axis=1)

        return normalized_user_hashtag_matrix, hashtags_topics_matrix, users_index

    @classmethod
    def get_matrix_from_data(cls, data):
        table = pd.DataFrame(data, columns=['x', 'y', 'weight'])
        return csr_matrix((table.weight, (table.x, table.y)))

    @classmethod
    def save_matrix(cls, matrix):
        """ Method which saves tf-idf matrix"""
        date = datetime.datetime.today()
        save_npz(f'{str(date.year)}-{str(date.month)}-{str(date.day)}', matrix)

    @classmethod
    def get_grouped_users(cls, users_index):
        """ Return users grouped by candidates' support. """
        # Retrieve users which have tweets
        active_users = RawFollowerDAO().get_all({'probability_vector_support': {'$exists': True}})
        users_by_group = {}
        for user in active_users:
            support_vector = user['probability_vector_support']
            max_probability_support = max(support_vector)

            # User who have not one probability greater than limit, is discarded
            if max_probability_support < 0.6:
                continue

            support_index = support_vector.index(max_probability_support)
            users_by_group[support_index] = users_by_group.get(support_index, []).append(
                [0, users_index[user['_id']], 1])
        return users_by_group

    @classmethod
    def get_group_matrix(cls, tf_idf_matrix, group_vector):
        """ Return group matrix without 0's"""
        group_matrix = tf_idf_matrix.multiply(group_vector)
        return group_matrix[group_matrix.getnnz(1) > 0]

    @classmethod
    def get_logger(cls):
        return Logger('UserTopicService')
