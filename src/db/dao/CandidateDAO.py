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
        as_dict = self.get_first({'screen_name': screen_name})
        if as_dict is None:
            raise NonExistentCandidateError(screen_name)
        return Candidate(**as_dict)

    def overwrite(self, candidate):
        """ Update candidate's fields (except for screen name). """
        self.update_first({'screen_name': candidate.screen_name},
                          {'nickname': candidate.nickname, 'last_updated_followers': candidate.last_updated_followers})

    def save(self, candidate):
        """ Store candidate. """
        return self.insert(vars(candidate))

    def all(self):
        """ Get all currently stored candidates. """
        candidates = []
        as_dict_list = self.get_all()
        for as_dict in as_dict_list:
            candidates.append(Candidate(**as_dict))
        return candidates

    def create_indexes(self):
        self.logger.info('Creating screen_name index.')
        Mongo().get().db.candidates.create_index('screen_name')

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
            self.insert(candidate)
