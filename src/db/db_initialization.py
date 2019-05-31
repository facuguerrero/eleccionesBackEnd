import datetime

from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.service.tweets.FollowersQueueService import FollowersQueueService


def create_indexes():
    """ Create all required collection indexes. """
    CandidateDAO().create_indexes()
    RawFollowerDAO().create_indexes()


def create_base_entries():
    """ Create all required entries. """
    CandidateDAO().create_base_entries()


def create_queue_entries():
    """ Add followers to download's queue. """
    FollowersQueueService().add_followers_to_be_updated()


def fix_followers_update():
    followers = RawFollowerDAO().get_all( {'downloaded_on': {'$gt': datetime.datetime(2019, 5, 29, 0, 0, 0)}} )
    for follower in followers:
        real_follows = []
        for seguido in follower['follows']:
            if isinstance(seguido,str):
                real_follows.append(seguido)
        RawFollowerDAO().update_follows(follower['_id'], real_follows)
