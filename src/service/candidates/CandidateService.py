from datetime import datetime

from src.db.dao.CandidateDAO import CandidateDAO
from src.exception.CandidateCurrentlyAvailableForUpdateError import CandidateCurrentlyAvailableForUpdateError
from src.exception.FollowerUpdatingNotNecessaryError import FollowerUpdatingNotNecessaryError
from src.util.DateUtils import DateUtils
from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CandidateService(metaclass=Singleton):

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.updating_followers = set()
        self.candidates = []
        # Load candidates from db and create objects to access their elements
        self.candidates = CandidateDAO().all()
        ConcurrencyUtils().create_lock('candidate_for_update')

    def get_all(self):
        """ Returns all candidates currently in the list. """
        return self.candidates

    def get_for_follower_updating(self):
        """ Polls a candidate for updating its follower list. """
        # Lock to avoid concurrency issues when retrieving candidates across threads
        ConcurrencyUtils().acquire_lock('candidate_for_update')
        for candidate in self.candidates:
            # We will only return a candidate if it was not updated today and is not being currently updated
            if candidate not in self.updating_followers and not DateUtils.is_today(candidate.last_updated_followers):
                self.logger.info(f'Returning candidate {candidate.screen_name} for follower retrieval.')
                self.updating_followers.add(candidate)
                # Unlock
                ConcurrencyUtils().release_lock('candidate_for_update')
                return candidate
        # Unlock
        ConcurrencyUtils().release_lock('candidate_for_update')
        raise FollowerUpdatingNotNecessaryError()

    def finish_follower_updating(self, candidate):
        """ Unlock user for follower updating and update last updating time. """
        if candidate not in self.updating_followers:
            raise CandidateCurrentlyAvailableForUpdateError(candidate.screen_name)
        # Update last updated followers date
        self.logger.info(f'Removing candidate {candidate.screen_name} from currently updating set.')
        candidate.last_updated_followers = datetime.now()
        CandidateDAO().overwrite(candidate)
        # Remove from set to not be polled again
        self.updating_followers.remove(candidate)
