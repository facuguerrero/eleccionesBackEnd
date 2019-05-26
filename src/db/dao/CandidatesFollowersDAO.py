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
        self.upsert({'_id': candidate_name},
                    {'$addToSet': {'increases': increase_object}})

    def get_increases_for_candidate(self, candidate_name):
        """ Get all increases for a given candidate. """
        document = self.get_first({'_id': candidate_name},
                                  {'_id': 0})
        if document is not None:
            # Map date to timestamp to send inside a JSON object
            document['increases'] = [{'count': increase['count'], 'date': increase['date'].timestamp()}
                                     for increase in document['increases']]
            return document['increases']
        else:
            raise NoDocumentsFoundError(collection_name='candidates_followers', query=f'screen_name={candidate_name}')

    def get_all_increases(self):
        """ Get all increases for all candidates. """
        documents = self.get_all()
        # Rename _id field for every document and map date to timestamp
        return [{'screen_name': document['_id'],
                 'increases': [{'count': increase['count'], 'date': increase['date'].timestamp()}
                               for increase in document['increases']]}
                for document in documents]
