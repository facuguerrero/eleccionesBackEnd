import csv
import datetime
from os.path import abspath, join, dirname
from threading import Thread

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, save_npz
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.preprocessing import normalize

from src.db.dao.CooccurrenceGraphDAO import CooccurrenceGraphDAO
from src.db.dao.HashtagsTopicsDAO import HashtagsTopicsDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO
from src.exception.NonExistentDataForMatrixError import NonExistentDataForMatrixError
from src.util.logging.Logger import Logger
from src.util.slack.SlackHelper import SlackHelper

SAVE_PATH = f"{abspath(join(dirname(__file__), '../../../../'))}/data/"


class UserTopicService:
    # TODO calcular la similitud random a partir de las similitudes ya calculadas.
    # Hay que hacer algo parecido a lo que se hace en el algoritmo para multiplicar la matriz.
    # Calcular la similitud random ponderada, es decir la sumatoria de
    # la similitud calculada * cantidad de usuarios * cantidad usuarios -1 /2

    @classmethod
    def init_update_support_follower(cls):
        thread = Thread(target=UserTopicService.init_process)
        thread.start()

    @classmethod
    def init_process(cls):
        try:
            cls.get_logger().info("Calculating User-Topic Matrix")
            cls.calculate_users_similarity(datetime.datetime(2019, 8, 1))
        except NonExistentDataForMatrixError as e:
            cls.get_logger().error("Error Calculating User-Topic Matrix. No data are retrieved.")
            SlackHelper().post_message_to_channel(
                f'Fallo el calculo de similitudes. No se obtuvo data para la matriz {e.matrix}.', '#errors')
        except Exception as e:
            cls.get_logger().error("Error Calculating User-Topic Matrix")
            cls.get_logger().error(e)
            # SlackHelper().post_message_to_channel('Fallo el update de follower support.', '#errors')

    @classmethod
    def calculate_users_similarity(cls, date):
        """ Method for calculate and update users similarity. """
        users_topic_matrix, users_index = cls.calculate_and_save_users_topics_matrix(date)
        users_quantity = users_topic_matrix.get_shape()[0]
        users_by_group = cls.get_grouped_users(users_index)

        grouped_matrixes = []
        for group in users_by_group:
            grouped_matrixes.append(cls.get_matrix_by_group(users_topic_matrix, group, users_quantity))

    @classmethod
    def calculate_and_save_users_topics_matrix(cls, date):
        """ This method calculate the user-topic matrix. """

        # Retrieve necessaries data
        users_hashtags_matrix, hashtags_topics_matrix, users_index = cls.get_necessary_data(date)
        cls.get_logger().info("Data retrieved correctly.")

        # Multiply this matrix and get users_topics matrix
        users_topics_matrix = users_hashtags_matrix * hashtags_topics_matrix
        cls.get_logger().info(f"Users_topics matrix created correctly. {users_topics_matrix.get_shape()}")

        # Retrieve only 5% most used topics
        users_topics_matrix = cls.get_matrix_with_most_used_topics(users_topics_matrix)
        cls.get_logger().info(f"Topics are cleaned. {users_topics_matrix.get_shape()}")
        # Apply TF-IDF
        tfidf_transformer = TfidfTransformer()
        tf_idf_matrix = tfidf_transformer.fit_transform(users_topics_matrix)
        # Axis 1 normalize by row
        normalized_user_hashtag_matrix = normalize(tf_idf_matrix, norm='l2', axis=1)
        # Clean matrix
        clean_matrix = normalized_user_hashtag_matrix[normalized_user_hashtag_matrix.getnnz(1) > 0]
        clean_matrix = clean_matrix.astype("float32")
        new_users_index = cls.get_new_users_index(normalized_user_hashtag_matrix, users_index)
        cls.get_logger().info(
            f"Users-Topics clean Matrix Dimentions: {clean_matrix.get_shape()} and users_index {len(new_users_index)}")

        # Save matrix
        cls.save_data(clean_matrix, new_users_index, date)
        cls.get_logger().info("Finished process. Data are saved correctly.")
        return clean_matrix, new_users_index

    @classmethod
    def get_necessary_data(cls, date):
        """ Retrieve from database all necessary data.
        user_hashtag matrix
        hashtag_topic matrix
        user_index are the user's row position in user_hashtag_matrix
        """

        # Retrieve last 3 days hashtags list sorted alphabetically
        last_3_days_hashtags, users_with_hashtags = UserHashtagDAO().get_last_10_days_hashtags(date)
        hashtags_quantity = len(last_3_days_hashtags)
        users_quantity = len(users_with_hashtags)
        cls.get_logger().info(
            f"All hashtags from 10 days ago are retrieved. They are {hashtags_quantity} from {users_quantity} users.")

        # Get an auxiliary structure
        hashtags_index = cls.get_hashtags_index(last_3_days_hashtags)

        # Get users-hashtags data and users index structure {user: matrix_index}
        # Users_index are the user's row in matrix
        users_hashtags_data, users_index = UserHashtagDAO().get_last_10_days_users_and_hashtags(hashtags_index)
        if len(users_hashtags_data) == 0: raise NonExistentDataForMatrixError("User-Hashtag")
        users_hashtags_matrix = cls.get_matrix_from_data_with_dtype(users_hashtags_data, users_quantity,
                                                                    hashtags_quantity)
        cls.get_logger().info(f"Users-Hashtags Matrix dimentions: N {users_quantity}, M {hashtags_quantity}")

        # Get hashtags-topics matrix
        all_topics_sorted = CooccurrenceGraphDAO().get_all_sorted_topics()
        topics_quantity = len(all_topics_sorted)
        cls.get_logger().info(f"All topics retrieved. They are {topics_quantity}.")

        hashtags_topics_data = HashtagsTopicsDAO().get_required_hashtags(last_3_days_hashtags, hashtags_index, date)
        if len(hashtags_topics_data) == 0: raise NonExistentDataForMatrixError("Hashtag-Topic")
        hashtags_topics_matrix = cls.get_matrix_from_data_with_dtype(hashtags_topics_data, hashtags_quantity,
                                                                     topics_quantity)
        cls.get_logger().info(f"Hashtags-Topics Matrix dimentions: N {hashtags_quantity}, M {topics_quantity}")

        return users_hashtags_matrix, hashtags_topics_matrix, users_index

    @classmethod
    def get_hashtags_index(cls, hashtags):
        """ Return an auxiliary structure for optimize process."""
        hashtags_index = {}
        index = 0
        for hashtag in hashtags:
            hashtags_index[hashtag] = index
            index += 1
        return hashtags_index

    @classmethod
    def get_matrix_from_data_with_dtype(cls, data, M, N):
        return cls.get_matrix_from_data(data, M, N).asType("float32")

    @classmethod
    def get_matrix_from_data(cls, data, M, N):
        table = pd.DataFrame(data, columns=['x', 'y', 'weight'])
        return csr_matrix((table.weight, (table.x, table.y)), shape=(M, N))

    @classmethod
    def get_matrix_with_most_used_topics(cls, users_topics_matrix):
        """ Method which return user topics matrix with 5% most used topics. """
        sums = []
        for x in range(users_topics_matrix.shape[1]):
            sums.append(sum(users_topics_matrix.getcol(x).data))

        required_index = []
        value = np.percentile(sums, 95)
        for x in range(users_topics_matrix.shape[1]):
            if sum(users_topics_matrix.getcol(x).data) >= value:
                required_index.append(x)
        return users_topics_matrix.transpose()[required_index].transpose()

    @classmethod
    def get_new_users_index(cls, normalized_user_hashtag_matrix, users_index):
        """ Method which return an auxiliary structure. """
        index_user = {}
        for user in users_index.keys():
            user_index = users_index[user]
            index_user[user_index] = str(user)
        user_vectors = normalized_user_hashtag_matrix.getnnz(1)
        new_user_index = {}
        actual_index = 0
        for x in range(len(user_vectors)):
            vectors = user_vectors[x]
            if vectors != 0:
                user = index_user[x]
                new_user_index[str(user)] = actual_index
                actual_index += 1
        return new_user_index

    @classmethod
    def save_data(cls, matrix, users_index, date):
        """ Method which saves tf-idf matrix"""
        date_name = str(date.year) + str(date.month) + str(date.day)
        save_npz(SAVE_PATH + f'{date_name}-matrix', matrix)

        with open(SAVE_PATH + f'{date_name}-index-bis.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, val in users_index.items():
                writer.writerow([key, val])

    @classmethod
    def get_grouped_users(cls, users_index):
        """ Return users grouped by candidates' support. """
        # Retrieve users which have tweets
        active_users = RawFollowerDAO().get_all({
            "$and": [
                {"probability_vector_support": {"$elemMatch": {"$gte": 0.8}}},
                {"has_tweets": True}
            ]})
        users_by_group = {}
        for user in active_users:
            support_vector = user['probability_vector_support']
            max_probability_support = max(support_vector)
            user_id = user['_id']

            # User who have not one probability greater than limit, is discarded
            if max_probability_support <= 0.8 or user_id not in users_index:
                continue

            support_index = support_vector.index(max_probability_support)
            value = users_by_group.get(support_index, [])
            value.append([users_index[user['_id']], 0, 1])
            users_by_group[support_index] = value

        return users_by_group

    @classmethod
    def get_matrix_by_group(cls, matrix, group_vector, users_quantity):
        """ Return group matrix without 0's"""
        selected_users_vector = cls.get_matrix_from_data_with_dtype(group_vector, users_quantity, 1)

        group_matrix = matrix.multiply(selected_users_vector)
        group_matrix = group_matrix[group_matrix.getnnz(1) > 0]
        return group_matrix.astype("float32")

    @classmethod
    def get_logger(cls):
        return Logger('UserTopicService')
