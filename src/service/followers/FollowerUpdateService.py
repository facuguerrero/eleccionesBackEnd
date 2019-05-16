import time
from twython import Twython, TwythonRateLimitError
from datetime import datetime

from src.db.dao.CandidatesFollowersDAO import CandidatesFollowersDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.exception.CredentialsAlreadyInUseError import CredentialsAlreadyInUseError
from src.exception.FollowerUpdatingNotNecessaryError import FollowerUpdatingNotNecessaryError
from src.model.followers.RawFollower import RawFollower
from src.service.candidates.CandidateService import CandidateService
from src.service.credentials.CredentialService import CredentialService
from src.util.concurrency.AsyncThreadPoolExecutor import AsyncThreadPoolExecutor
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger


class FollowerUpdateService:

    LOGGER = Logger('FollowerUpdateService')

    @classmethod
    def update_followers(cls):
        """ Update all candidates' followers. """
        # Get credentials for service
        cls.LOGGER.info('Starting follower updating process.')
        try:
            credentials = CredentialService().get_all_credentials_for_service(cls.__name__)
        except CredentialsAlreadyInUseError as caiue:
            cls.LOGGER.error(caiue.message)
            cls.LOGGER.warning('Follower updating process skipped.')
            return
        # Run follower update process
        AsyncThreadPoolExecutor().run(cls.update_with_credential, credentials)
        cls.LOGGER.info('Finished follower updating.')

    @classmethod
    def update_with_credential(cls, credential):
        """ Update followers with an specific Twitter API credential. """
        cls.LOGGER.info(f'Starting follower updating with credential {credential.id}.')
        # Create Twython instance for credential
        twitter = cls.twitter(credential)
        # While there are candidates to update, get and update
        candidate = cls.next_candidate()
        while candidate is not None:
            # Update followers
            cls.update_followers_for_candidate(twitter, candidate)
            # Finish using candidate
            CandidateService().finish_follower_updating(candidate)
            # Get next candidate
            candidate = cls.next_candidate()
        # Unlock credential for this service
        CredentialService().unlock_credential(credential.id, cls.__name__)
        cls.LOGGER.info(f'Finished updating followers with credential {credential.id}')

    @classmethod
    def update_followers_for_candidate(cls, twitter, candidate):
        """ Update followers of given candidate with the given Twython instance. """
        cls.LOGGER.info(f'Follower updating started for candidate {candidate.screen_name}.')
        # Get already stored candidates
        candidate_followers_ids = RawFollowerDAO().get_candidate_followers_ids(candidate.screen_name)
        # Retrieve new candidates
        to_store_ids = cls.get_new_followers_ids(twitter, candidate, candidate_followers_ids)
        cls.LOGGER.info(f'{len(to_store_ids)} new followers downloaded for candidate {candidate.screen_name}.')
        # Once the downloading is done, we proceed to store the new followers
        cls.store_new_followers(to_store_ids, candidate.screen_name)
        cls.LOGGER.info(f'Finished updating followers for candidate {candidate.screen_name}.')

    @classmethod
    def get_new_followers_ids(cls, twitter, candidate, candidate_followers_ids):
        """ Use Twitter API to get new followers for given candidate. Checking for overlaps with the already stored
        followers. """
        twitter_response = cls.do_request(twitter, candidate.screen_name, 0)
        new_followers = cls.ids_to_string_set(twitter_response['ids'])
        next_cursor = twitter_response['next_cursor']
        while cls.should_retrieve_more_followers(candidate_followers_ids, new_followers) and next_cursor != -1:
            twitter_response = cls.do_request(twitter, candidate.screen_name, next_cursor)
            # Join this iteration's download with previous iteration's download
            new_followers = new_followers.union(cls.ids_to_string_set(twitter_response['ids']))
            next_cursor = twitter_response['next_cursor']
        return new_followers.difference(candidate_followers_ids)

    @classmethod
    def store_new_followers(cls, ids, candidate_name):
        """ Create RawFollower instances for the received data and store them in the database. Also, we will store
         the number of new followers downloaded each day. """
        today = datetime.today()
        # Create and store raw followers
        for follower_id in ids:
            raw_follower = RawFollower(**{'id': follower_id,
                                          'follows': candidate_name,
                                          'downloaded_on': today})
            RawFollowerDAO().put(raw_follower)
        # Store the number of retrieved followers in the current day
        count = len(ids)
        CandidatesFollowersDAO().put_increase_for_candidate(candidate_name, count, today)

    @classmethod
    def should_retrieve_more_followers(cls, previous, new):
        """ Determines if the currently downloaded users already cover the needed update. """
        return len(new.intersection(previous)) < ConfigurationManager().get_int('max_follower_overlap')

    @classmethod
    def next_candidate(cls):
        """ Retrieve a candidate whose followers should be updated. """
        try:
            candidate = CandidateService().get_for_follower_updating()
        except FollowerUpdatingNotNecessaryError:
            return None
        return candidate

    @classmethod
    def do_request(cls, twitter, candidate_name, cursor=0):
        """ Handle request to Twitter's API. """
        try:
            if cursor == 0:
                return twitter.get_followers_ids(screen_name=candidate_name)
            else:
                return twitter.get_followers_ids(screen_name=candidate_name, cursor=str(cursor))
        except TwythonRateLimitError:
            cls.LOGGER.warning(f'Follower download limit reached for candidate {candidate_name}. Waiting.')
            time.sleep(ConfigurationManager().get_int('follower_download_sleep_seconds'))
            cls.LOGGER.info(f'Waiting done. Resuming follower updating for candidate {candidate_name}.')
            # Once we finished waiting, we try again
            return cls.do_request(twitter, candidate_name, cursor)

    @staticmethod
    def ids_to_string_set(id_list):
        return {str(follower_id) for follower_id in id_list}

    @classmethod
    def twitter(cls, credential):
        """ Create Twython instance depending on credential data. """
        if credential.access_token is None:
            twitter = Twython(app_key=credential.consumer_key, app_secret=credential.consumer_secret)
        elif credential.consumer_key is None:
            twitter = Twython(oauth_token=credential.access_token, oauth_token_secret=credential.access_secret)
        else:
            twitter = Twython(app_key=credential.consumer_key, app_secret=credential.consumer_secret,
                              oauth_token=credential.access_token, oauth_token_secret=credential.access_secret)
        return twitter
