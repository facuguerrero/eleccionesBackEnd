import datetime

import pandas as pd
from scipy.sparse import csr_matrix, save_npz
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.preprocessing import normalize

from src.db.dao.CoocurrenceGraphsDAO import CoocurrenceGraphsDAO
from src.db.dao.HashtagsTopicsDAO import HashtagsTopicsDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO


class UserTopicService:

    @classmethod
    def calculate_users_topics_matrix(cls):
        """ This method calculate the user-topic matrix. """

        # Retrieve necessary data
        # Todo normalizar
        users_hashtags_matrix, hashtags_topics_matrix = cls.get_necessary_data()

        # Multiply this matrix and get users_topics matrix
        users_topics_matrix = users_hashtags_matrix.multiply(hashtags_topics_matrix)

        # Apply TF-IDF
        tfidf_transformer = TfidfTransformer()
        tf_idf_matrix = tfidf_transformer.fit_transform(users_topics_matrix)

        # Save matrix
        cls.save_matrix(tf_idf_matrix)

        # TODO seguir con eso
        # Agrupar a los usuarios por partido
        # Hacer con el producto con la traspuesta por cada par de matriz que encontremos
        # Hacer un histograma de la data de cada matriz
        # A esta matriz calcularle la media y guardarlo en una colección de mongo

        # Hacer el producto de 2 matrices de grupo de usuarios distintos
        # Tiene que haber una diferencia con el resto
        # Despues de multiplicarlas pueden quedar 0
        # Calcular el promedio con 0 y sin 0
        # No contarlos para dividir por el total.
        # Si tenemos 70 numeros, y 10 son 0 es como tener 60

    @classmethod
    def get_necessary_data(cls):
        """ Retrieve from database all necessary data. """

        # Retrieve all topics sorted list
        all_topics = CoocurrenceGraphsDAO().get_all_sorted_topics()

        # Retrieve all hashtags sorted alphabetically
        last_3_days_hashtags = UserHashtagDAO().get_last_3_days_hashtags()

        # Get users-hashtags matrix
        users_hashtags_data = UserHashtagDAO().get_last_3_days_users_and_hashtags(last_3_days_hashtags)
        users_hashtags_matrix = cls.get_matrix_from_data(users_hashtags_data)
        # Axis 1 normalize by row
        normalized_user_hashtag_matrix = normalize(users_hashtags_matrix, norm='l1', axis=1)

        # Get hashtags-topics matrix
        hashtags_topics_data = HashtagsTopicsDAO().get_required_hashtags(all_topics, last_3_days_hashtags)
        hashtags_topics_matrix = cls.get_matrix_from_data(hashtags_topics_data)

        return normalized_user_hashtag_matrix, hashtags_topics_matrix

    @classmethod
    def get_matrix_from_data(cls, data):
        table = pd.DataFrame(data, columns=['x', 'y', 'weight'])
        return csr_matrix((table.weight, (table.x, table.y)))

    @classmethod
    def save_matrix(cls, matrix):
        """ Method which saves tf-idf matrix"""
        date = datetime.datetime.today()
        save_npz(f'{str(date.year)}-{str(date.month)}-{str(date.day)}', matrix)
