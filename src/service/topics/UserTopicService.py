from src.db.dao.UserHashtagDAO import UserHashtagDAO


class UserTopicService:

    @classmethod
    def insert_hashtags_of_already_downloaded_tweets(cls):
        """"""

        # Pedir los topicos que se crearon

        # TODO preguntarle a rodri:
        # Se puede hacer algo parecido a lo que se hizo en  UserHashtagDAO() ??
        # Tener en memoria un dict { topico: [hashtags] } ???
        # Que dimensiones tiene eso?

        # Crear una lista inmutable que tenga los topicos ordenados alfabeticamente
        # hashtags_quantity = len( lista )
        hashtags_by_users = UserHashtagDAO().get_hashtags_aggregated_by_user()
        for user_with_hashtags in hashtags_by_users:
            user = user_with_hashtags['_id']
            hashtags = user_with_hashtags['hashtags_array']

            # Crear un vector de tamaño hashtags_quantity
            # user_hashtags_vector = [0] * hashtags_quantity
            # for hashtag in hashtags:
            # Pedir  a que topico pertenece el hashtag
            # sumar uno en esa posicion del vector resultado
