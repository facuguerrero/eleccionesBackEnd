from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NonExistentRawFollowerError import NonExistentRawFollowerError
from src.model.followers.RawFollower import RawFollower
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class RawFollowerDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(RawFollowerDAO, self).__init__(Mongo().get().db.raw_followers)
        self.logger = Logger(self.__class__.__name__)

    def put(self, raw_follower):
        """ Adds RawFollower to data base using upsert to update 'follows' list."""
        self.upsert({'id': raw_follower.id},
                    {'$addToSet': {'follows': raw_follower.follows},
                     '$set': {'downloaded_on': raw_follower.downloaded_on}
                     })

    def get(self, follower_id):
        as_dict = self.get_first({'id': follower_id})
        if as_dict is None:
            raise NonExistentRawFollowerError(follower_id)
        return RawFollower(**as_dict)

    def finish_candidate(self, candidate_name):
        """ Add entry to verify if a certain candidate had its followers loaded. """
        self.insert({'id': candidate_name})

    def candidate_was_loaded(self, candidate_name):
        """ Verify if a given candidate had its followers loaded. """
        return self.get_first({'id': candidate_name}) is not None

    def get_candidate_followers_ids(self, candidate_name):
        """ Retrieve all the ids of the users that follow a given candidate. """
        ids = self.get_all({'follows': candidate_name}, {'id': 1, '_id': 0})
        # We need to extract the element from the document because of the format they come in
        return {document['id'] for document in ids}

    def create_indexes(self):
        self.logger.info('Creating id index.')
        Mongo().get().db.raw_followers.create_index('id')
