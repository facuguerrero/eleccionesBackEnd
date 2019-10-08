from src.db.dao.PartyRelationshipsDAO import PartyRelationshipsDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.UsersFriendsDAO import UsersFriendsDAO
from src.util.logging.Logger import Logger


class UserNetworkAnalysisService:
    __parties = ['juntosporelcambio', 'frentedetodos', 'frentedespertar', 'consensofederal', 'frentedeizquierda']

    @classmethod
    def calculate_relationships(cls):
        users_by_party = cls.populate_users_by_party_dict()
        for party in cls.__parties:
            # Get normalized vector for the given party
            normalized_vector, summed_vector, users_count = cls.calculate_relationships_for_party(party, users_by_party)
            # Store party vector for today
            PartyRelationshipsDAO().store(party, normalized_vector, summed_vector, users_count)

    @classmethod
    def calculate_relationships_for_party(cls, party, users_by_party):
        friends_lists = UsersFriendsDAO().get_users_for_party(party)
        # List of user vectors
        vectors = list()
        # Iterate all lists of friends and set number of known users it follows
        for friends_list in friends_lists:
            user_vector = list()
            # Add the count of users from each party this user follows
            for party in cls.__parties + ['unknown']:
                user_vector.append(len(users_by_party[party].intersection(set(friends_list))))
            # Add user vector to the party's vector
            vectors.append(user_vector)
        # Sum all vector's values
        summed_vector = [sum(x) for x in zip(*vectors)]
        # Get sum of values to normalize
        total_edges = sum(summed_vector)
        # Normalize vector and return
        return [x / total_edges for x in summed_vector], summed_vector, len(friends_lists)

    @classmethod
    def populate_users_by_party_dict(cls):
        users_by_party = dict()
        for party in cls.__parties:
            documents = RawFollowerDAO().get_all({
                '$and': [{'probability_vector_support': {'$elemMatch': {'$gte': 0.8}}}, {'support': party}]
            })
            # Store list in party dictionary
            users_by_party[party] = {document['_id'] for document in documents}
        # Get the users we don't know which party they support
        documents = RawFollowerDAO().get_all({'probability_vector_support': {'$elemMatch': {'$lt': 0.8}}})
        users_by_party['unknown'] = {document['_id'] for document in documents}
        return users_by_party

    @classmethod
    def get_logger(cls):
        return Logger(cls.__name__)
