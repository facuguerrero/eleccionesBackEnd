from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CandidatesFollowersDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(CandidatesFollowersDAO, self).__init__(Mongo().get().db.candidates_followers)
        self.logger = Logger(self.__class__.__name__)

    def put_increase_for_candidate(self, candidate_name, count, date):
        """ Add increase object to set of given candidate. """
        increase_object = {'date': date, 'count': count}
        self.upsert({'screen_name': candidate_name},
                    {'$addToSet': {'increases': increase_object}})

    def get_increases_for_candidate(self, candidate_name):
        """ Get all increases for a given candidate. """
        document = self.get_first({'screen_name': candidate_name},
                                  {'_id': 0, 'screen_name': 0})
        if document is not None:
            return document['increases']
        else:
            raise NoDocumentsFoundError(collection_name='candidates_followers', query=f'screen_name={candidate_name}')
