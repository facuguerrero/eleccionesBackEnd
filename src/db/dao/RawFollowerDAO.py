import datetime

import pytz

from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.exception.NoDocumentsFoundError import NoDocumentsFoundError
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
        self.upsert({'_id': raw_follower.id},
                    {'$addToSet': {'follows': raw_follower.follows},
                     '$set': {'downloaded_on': raw_follower.downloaded_on},
                     # This field is ignored if it already exists
                     '$setOnInsert': {'is_private': raw_follower.is_private}
                     })

    def tag_as_private(self, raw_follower):
        """ Tags the given user as private in the database. """
        self.upsert({'_id': raw_follower.id},
                    {'$set': {'is_private': True}})

    def get(self, follower_id):
        as_dict = self.get_first({'_id': follower_id})
        if as_dict is None:
            raise NonExistentRawFollowerError(follower_id)
        # Transform from DB format to DTO format
        as_dict['id'] = as_dict.pop('_id', None)
        return RawFollower(**as_dict)

    def get_public_users(self):
        """ Retrieve all the ids of the users that are not catalogued as private. """
        documents = self.get_all({'is_private': False}, {'_id': 1})
        # We need to extract the element from the dictionary
        return {document['_id'] for document in documents}

    def get_public_and_not_updated_users(self):
        """ Retrieve all the ids of the users that are not updated since one week catalogued as private
        . """
        """ Retrieve all the ids of the users that follow a given candidate. """
        # TODO llevar el tiempo a
        date = datetime.datetime.today() - datetime.timedelta(days=7)
        documents = self.get_with_limit({'is_private': False, 'downloaded_on': {'$lt': date}},
                                        {'_id': 1, 'downloaded_on': 1})
        followers_to_return = {}
        for document in documents:
            followers_to_return[document['_id']] = document['downloaded_on']
        return followers_to_return

    def finish_candidate(self, candidate_name):
        """ Add entry to verify if a certain candidate had its followers loaded. """
        self.insert({'_id': candidate_name})

    def candidate_was_loaded(self, candidate_name):
        """ Verify if a given candidate had its followers loaded. """
        return self.get_first({'_id': candidate_name}) is not None

    def get_candidate_followers_ids(self, candidate_name):
        """ Retrieve all the ids of the users that follow a given candidate. """
        documents = self.get_all({'follows': candidate_name}, {'_id': 1})
        # We need to extract the element from the document because of the format they come in
        return {document['_id'] for document in documents}

    def get_all_with_cursor(self, start, limit):
        """ Get all raw_follower documents using the received information as cursor. """
        documents = self.get_with_cursor(sort='_id', skip=start, limit=limit)
        # Map documents for return.
        return [RawFollower(**{'id': document['_id'],
                               'follows': document['follows'],
                               'downloaded_on': document['downloaded_on'],
                               'is_private': document['is_private']})
                for document in documents]

    def get_following_with_cursor(self, candidate_name, start, limit):
        """ Retrieve all raw_followers who follow a given candidate with a cursor. """
        documents = self.get_with_cursor({'follows': candidate_name}, sort='_id', skip=start, limit=limit)
        # Raise error if there are no documents for that candidate
        if documents.count() == 0:
            raise NoDocumentsFoundError(collection_name='raw_followers', query=f'screen_name={candidate_name}')
        # Map documents for return and remove downloaded date.
        return [RawFollower(**{'id': document['_id'],
                               'follows': document['follows'],
                               'downloaded_on': document['downloaded_on'],
                               'is_private': document['is_private']})
                for document in documents]

    def create_indexes(self):
        self.logger.info('Creating is_private index for collection raw_followers.')
        Mongo().get().db.raw_followers.create_index('is_private')
