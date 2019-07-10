from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.CooccurrenceDAO import CooccurrenceDAO
from src.db.dao.CooccurrenceGraphDAO import CooccurrenceGraphDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO
from src.service.queue_followers.FollowersQueueService import FollowersQueueService


def create_indexes():
    """ Create all required collection indexes. """
    CandidateDAO().create_indexes()
    RawFollowerDAO().create_indexes()
    RawTweetDAO().create_indexes()
    UserHashtagDAO().create_indexes()
    CooccurrenceGraphDAO().create_indexes()
    CooccurrenceDAO().create_indexes()


def create_base_entries():
    """ Create all required entries. """
    CandidateDAO().create_base_entries()


def create_queue_entries():
    """ Add followers to download's queue. """
    # FollowersQueueService().add_followers_to_be_updated()
    FollowersQueueService().add_last_downloaded_followers()
