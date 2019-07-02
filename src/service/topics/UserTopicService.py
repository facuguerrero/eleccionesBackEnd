import datetime

import numpy as np
from scipy.sparse import csr_matrix, save_npz
from sklearn.feature_extraction.text import TfidfTransformer

from src.db.dao.CoocurrenceGraphsDAO import CoocurrenceGraphsDAO
from src.db.dao.HashtagsTopicsDAO import HashtagsTopicsDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO


class UserTopicService:

    @classmethod
    def calculate_users_topics_matrix(cls):
        """ This method calculate the user-topic matrix. """

        # Retrieve necessary data
        hashtags_by_users, topics_by_hashtags, all_topics = cls.get_necessary_data()
        hashtags_quantity = len(all_topics)

        # Initialise row's vector matrix and data vector
        users_row = []
        data = []

        for user, hashtags in hashtags_by_users.items():
            user_hashtags_vector = [0] * hashtags_quantity
            # For all user's hashtags
            for hashtag in hashtags:
                # Get the hashtag's topics
                topics_hashtag = topics_by_hashtags.get(hashtag, [])
                # For all hashtag's topics
                for topic in topics_hashtag:
                    index = all_topics.index(topic)
                    user_hashtags_vector[index] += 1

            if sum(user_hashtags_vector) > 0:
                # guardar el vector en la matriz
                users_row.append(user)
                data.append(user_hashtags_vector)

        cls.create_matrix_and_save(data)

    @classmethod
    def get_necessary_data(cls):
        """ Retrieve from database all necessary data. """

        # Retrieve yesterday's hashtags gruped by user
        # { 'user_id': [ hashtags ] }
        hashtags_by_users = UserHashtagDAO().get_hashtags_yesterday_aggregated_by_user()

        # Create hashtag's list and retrieve their topics
        # { 'hashtag_id' [topics] }
        yesterday_hashtags = cls.get_yesterday_hashtags(hashtags_by_users)
        hashtags_by_topics = HashtagsTopicsDAO().get_required_hashtags(yesterday_hashtags)

        # Retrieve all topics
        all_topics = CoocurrenceGraphsDAO().get_all_sorted_topics()

        return hashtags_by_users, hashtags_by_topics, all_topics

    @classmethod
    def get_yesterday_hashtags(self, hashtags_by_users):
        total_hashtags = set()
        for hashtags in hashtags_by_users.values():
            total_hashtags.update(hashtags)
        return list(total_hashtags)

    @classmethod
    def create_matrix_and_save(cls, data):
        """ Method which receive vector data and create a csr matrix.
        After that apply TF-IDF and save it."""
        # Create np array to be used in the matrix
        np_data = np.array(data)
        matrix = csr_matrix(np_data)

        # Apply TF-IDF
        tfidf_transformer = TfidfTransformer()
        tf_idf_matrix = tfidf_transformer.fit_transform(matrix)

        date = datetime.datetime.today()
        save_npz(f'{str(date.year)}-{str(date.month)}-{str(date.day)}', tf_idf_matrix)
