from src.db.dao.PartyRelationshipsDAO import PartyRelationshipsDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.UsersFriendsDAO import UsersFriendsDAO


class UserNetworkAnalysisService:

    __parties = ['juntosporelcambio', 'frentedetodos', 'frentedespertar', 'consensofederal', 'frentedeizquierda']

    users_by_party = dict()

    @classmethod
    def get_vectors_by_party(cls):
        vectors = dict()
        for party in cls.__parties:
            vectors[party] = PartyRelationshipsDAO().last_party_vector(party)
        return vectors

    @classmethod
    def calculate_relationships(cls):
        cls.populate_users_by_party_dict()
        for party in cls.__parties:
            # Get normalized vector for the given party
            result_vector = cls.calculate_relationships_for_party(party)
            # Store party vector for today
            PartyRelationshipsDAO().store(party, result_vector)

    @classmethod
    def calculate_relationships_for_party(cls, party):
        friends_lists = UsersFriendsDAO().get_users_for_party(party)
        # List of user vectors
        vectors = list()
        # Iterate all lists of friends and set number of known users it follows
        for friends_lists in friends_lists:
            user_vector = list()
            # Add the count of users from each party this user follows
            for party in cls.__parties:
                user_vector.append(len(cls.users_by_party[party].intersection(set(friends_lists))))
            # Add user vector to the party's vector
            vectors.append(user_vector)
        # Sum all vector's values
        summed_vector = [sum(x) for x in zip(*vectors)]
        # Get sum of values to normalize
        total_edges = sum(summed_vector)
        # Normalize vector and return
        return [x/total_edges for x in summed_vector]

    @classmethod
    def populate_users_by_party_dict(cls):
        for party in cls.__parties:
            documents = RawFollowerDAO().get_all({
                '$and': [
                    {'support': party},
                    {'probability_vector_support': {'$gte': 0.8}}
                ]},
                {'_id': 1})
            # Store list in party dictionary
            cls.users_by_party[party] = {document['_id'] for document in documents}
        return cls.users_by_party
