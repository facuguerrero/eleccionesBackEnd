from src.db.Mongo import Mongo
from src.db.dao.HashtagEntropyDAO import HashtagEntropyDAO
from src.util.config.ConfigurationManager import ConfigurationManager


class HashtagEntropyService:

    def __init__(self):
        self.filtered_hashtags = set()
        self.non_filtered_hashtags = set()

    def should_use_pair(self, pair, method):
        """ Returns true if both hashtags should be considered in graph creation. """
        for hashtag in pair:
            # Cache to avoid accessing DB
            if hashtag in self.filtered_hashtags:
                return False
            if hashtag in self.non_filtered_hashtags:
                continue
            # Search database for entropy vector
            document = HashtagEntropyDAO().find(hashtag)
            if document and self.__should_filter(document['vector'], method):
                self.filtered_hashtags.add(hashtag)
                key = f'{hashtag}-{method}'
                Mongo().db.filtered.find_one_and_update(filter={'_id': key}, update={'method': method}, upsert=True)
                return False
            self.non_filtered_hashtags.add(hashtag)
        return True

    @classmethod
    def __should_filter(cls, vector, method):
        """ Checks the vector values to verify if the associated hashtag should be used in graph creation or not. """
        return cls.__filter_with_index({
            'n5': 4,
            'n4': 3,
            'n3': 2,
            'n2': 1,
            'n1': 0
        }[method], vector)

    @classmethod
    def __filter_with_index(cls, index, vector):
        """ Accepts only the vectors that have proportions distributed through all indexes. """
        lower_bound = ConfigurationManager().get_float(f'n{index+1}_lower_bound')
        vector.sort(reverse=True)
        if index == 0:
            delta = vector[0]
        else:
            delta = vector[0] - vector[index]
        return delta > lower_bound
