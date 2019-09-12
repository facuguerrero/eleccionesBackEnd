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
from src.db.dao.SimilarityDAO import SimilarityDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO
from src.exception.NonExistentDataForMatrixError import NonExistentDataForMatrixError
from src.model.Similarities import Similarities
from src.util.logging.Logger import Logger
from src.util.slack.SlackHelper import SlackHelper

SAVE_PATH = f"{abspath(join(dirname(__file__), '../../../../'))}/data/"
REFERENCE = {'0': 'frentedetodos', '1': 'juntosporelcambio', '2': 'consensofederal', '3': 'frentedespertar',
             '4': 'frentedeizquierda'}


class UserTopicService:

    @classmethod
    def init_update_support_follower(cls):
        thread = Thread(target=UserTopicService.init_process)
        thread.start()

    @classmethod
    def init_process(cls):
        cls.init_process_with_date(datetime.datetime.today())
        # cls.init_process_with_date(datetime.datetime(2019, 7, 23))
        # cls.init_process_with_date(datetime.datetime(2019, 8, 15))

    @classmethod
    def init_process_with_date(cls, date):
        try:
            cls.get_logger().info(f"Calculating User-Topic Matrix for {str(date)}")
            cls.calculate_users_similarity(date)
            SlackHelper().post_message_to_channel(f'Similitud calculada correctamente {str(date)}', '#reports')
        except NonExistentDataForMatrixError as e:
            cls.get_logger().error("Error Calculating User-Topic Matrix. No data are retrieved.")
            SlackHelper().post_message_to_channel(
                f'Fallo el calculo de similitudes. No se obtuvo data para la matriz {e.matrix} en el día {str(date)}.',
                '#errors')
        except Exception as e:
            cls.get_logger().error("Error Calculating User-Topic Matrix")
            cls.get_logger().error(e)
            SlackHelper().post_message_to_channel(f'Fallo el calculo de la similitud para el día {str(date)}.',
                                                  '#errors')

    @classmethod
    def calculate_users_similarity(cls, date):
        """ Method for calculate and update users similarity. """
        users_topic_matrix, users_index = cls.calculate_and_save_users_topics_matrix(date)
        users_quantity = users_topic_matrix.get_shape()[0]
        users_by_group = cls.get_grouped_users(users_index)

        # Separate users by support
        grouped_matrices = []
        for group in sorted(users_by_group.keys()):
            matrix_by_group = cls.get_matrix_by_group(users_topic_matrix, users_by_group[group], users_quantity)
            grouped_matrices.append(cls.get_sliced_matrix(matrix_by_group))
        cls.get_logger().info('All matrix by group are calculated and sliced correctly.')

        # Calculate similarity between all groups
        means = []
        totals = []
        similarities = Similarities(str(date.year) + str(date.month) + str(date.day))
        groups_quantity = len(grouped_matrices)
        for x in range(groups_quantity):
            m1 = grouped_matrices[x]
            for y in range(x, groups_quantity):
                m2 = grouped_matrices[y]
                mean, total = cls.multiply_matrices_and_get_mean(m1, m2, x == y)

                means.append(mean)
                totals.append(total)

                similarities.add_similarity(f"{x}-{y}", mean)
                # cls.get_logger().info(f'Similarity between {x} - {y}: {mean}')

        random_mean = cls.get_weighted_mean(means, totals)
        similarities.add_similarity('random', random_mean)
        # cls.get_logger().info(f'Random {random_mean}')

        similarities_wor = {}
        for groups, sim in similarities.similarities.items():
            if groups != 'random':
                sliced_key = groups.split('-')
                new_key = REFERENCE[sliced_key[0]] + '-' + REFERENCE[sliced_key[1]]
                similarities_wor[new_key] = sim - random_mean

        similarities.set_similarities_wor(similarities_wor)
        SimilarityDAO().insert_similarities(similarities)
        cls.get_logger().info('All similarities are calculated correctly.')

    @classmethod
    def multiply_matrices_and_get_mean(cls, m1, m2, setdiag):
        """ Multiply 2 large matrices and return the result's mean. """
        partial_means = []
        partial_totals = []

        # Multiply every matrix's part
        for slice_m1 in m1:
            for slice_m2 in m2:
                partial_matrix_result = slice_m1.dot(slice_m2.transpose()).astype(dtype='float32')

                # Eliminate similarity between same users
                if setdiag:
                    partial_matrix_result.setdiag(0)

                # Calculate old_shape because after eliminate 0's a row could disappear
                old_shape = partial_matrix_result.shape
                partial_matrix_result.eliminate_zeros()
                new_shape = partial_matrix_result.shape[0]
                slices = cls.get_bounds(new_shape)

                for x in range(len(slices) - 1):
                    sliced_matrix = partial_matrix_result[slices[x]: slices[x + 1] - 1]
                    rows_quantity = slices[x + 1] - slices[x]
                    len_sliced_matrix = len(sliced_matrix.data)

                    partial_means.append(
                        sliced_matrix.mean(dtype='float16') * old_shape[1] * rows_quantity / len_sliced_matrix)
                    partial_totals.append(len_sliced_matrix)
                del partial_matrix_result

        return cls.get_weighted_mean(partial_means, partial_totals), sum(partial_totals)

    @classmethod
    def get_weighted_mean(cls, means, totals):
        mean = 0
        for x in range(len(means)):
            mean += means[x] * totals[x]
        return mean / sum(totals)

    @classmethod
    def calculate_and_save_users_topics_matrix(cls, date, have_to_save=True):
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
        if have_to_save: cls.save_data(clean_matrix, new_users_index, date)
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
        return cls.get_matrix_from_data(data, M, N).astype("float32")

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

        # para probar {"important": {'$exists': False}}
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
        selected_users_vector = cls.get_matrix_from_data(group_vector, users_quantity, 1)

        group_matrix = matrix.multiply(selected_users_vector)
        group_matrix = group_matrix[group_matrix.getnnz(1) > 0]
        return group_matrix.astype("float32")

    @classmethod
    def get_sliced_matrix(cls, matrix):
        """ If matrix dimention is greater than 20000, the matrix is sliced. """
        M = matrix.get_shape()[0]

        if M < 20000:
            # cls.get_logger().info(f'Matrix are not sliced. {M}')
            return [matrix]

        bounds = cls.get_bounds(M, int(M / 20000))
        sliced_matrix = []
        for x in range(len(bounds) - 1):
            sliced_matrix.append(matrix[bounds[x]: (bounds[x + 1] - 1)])
            # cls.get_logger().info(f'Matrix bounds {bounds[x]} - {bounds[x + 1] - 1}')
        return sliced_matrix

    @classmethod
    def get_bounds(cls, M, slices=4):
        limit = int(M / slices)
        bounds = [0]
        for x in range(1, slices):
            bounds.append(limit * x)
        bounds.append(M + 1)
        return bounds

    @classmethod
    def get_logger(cls):
        return Logger('UserTopicService')
