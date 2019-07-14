import json
from os.path import abspath, join, dirname

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NonExistentCandidateError import NonExistentCandidateError
from src.model.Candidate import Candidate
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CandidateDAO(GenericDAO, metaclass=Singleton):

    FILE_PATH = f"{abspath(join(dirname(__file__), '../../'))}/resources/candidates.json"

    def __init__(self):
        super(CandidateDAO, self).__init__(Mongo().get().db.candidates)
        self.logger = Logger(self.__class__.__name__)

    def find(self, screen_name):
        """ Get user with given screen name. """
        as_dict = self.get_first({'_id': screen_name})
        if as_dict is None:
            raise NonExistentCandidateError(screen_name)
        # Transform from DB format to DTO format
        as_dict['screen_name'] = as_dict['_id']
        return Candidate(**as_dict)

    def overwrite(self, candidate):
        """ Update candidate's fields (except for screen name). """
        self.update_first({'_id': candidate.screen_name},
                          {'nickname': candidate.nickname, 'last_updated_followers': candidate.last_updated_followers})

    def save(self, candidate):
        """ Store candidate. """
        # Transform from DTO format to DB format
        to_insert = {'_id': candidate.screen_name,
                     'nickname': candidate.nickname,
                     'last_updated_followers': candidate.last_updated_followers}
        return self.insert(to_insert)

    def all(self):
        """ Get all currently stored candidates. """
        candidates = []
        as_dict_list = self.get_all()
        for as_dict in as_dict_list:
            # Transform from DB format to DTO format
            as_dict['screen_name'] = as_dict['_id']
            candidates.append(Candidate(**as_dict))
        return candidates

    def create_indexes(self):
        # There are no indexes to create for this collection
        pass

    def create_base_entries(self):
        # Check if collection is empty
        if self.get_all().count() > 0:
            return
        # Load candidates
        self.logger.info('Loading candidates from file into database.')
        with open(CandidateDAO.FILE_PATH, 'r') as file:
            candidates = json.load(file)
        # Store entries
        for candidate in candidates:
            # Transform for database format
            to_insert = {'_id': candidate['screen_name'], 'nickname': candidate['nickname']}
            self.insert(to_insert)

    def update_json_resource(self, candidate):
        """ Add candidate to json file. """
        self.logger.info(f'Storing candidate {candidate.screen_name} into file.')
        with open(CandidateDAO.FILE_PATH, 'r') as file:
            candidates = json.load(file)
        # Append new candidate
        candidates.append({'screen_name': candidate.screen_name, 'nickname': candidate.nickname})
        # Write to file
        with open(CandidateDAO.FILE_PATH, 'w') as file:
            json.dump(candidates, file)

    def get_required_candidates(self):
        """ Retrieve dictionary like: {candidate: index}. """
        candidates = self.get_all({'index': {'$exists': True}})
        candidate_index = {}
        for candidate in candidates:
            candidate_index[candidate['_id']] = candidate['index']
        return candidate_index
