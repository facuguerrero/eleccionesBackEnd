from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.model.followers.RawFollower import RawFollower
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


def fix_int_ids():
    int_users = RawFollowerDAO().get_all({"_id": {"$type": 16}})
    for user_information in int_users:
        id_int = user_information.pop('_id')
        f = RawFollower(**{
            'id': str(id_int),
            'follows': ('follows' in user_information) if user_information['follows'] else '',
            'downloaded_on': ('downloaded_on' in user_information) if user_information['downloaded_on'] else '',
            'location': ('location' in user_information) if user_information['location'] else '',
            'followers_count': ('followers_count' in user_information) if user_information['followers_count'] else '',
            'friends_count': ('friends_count' in user_information) if user_information['friends_count'] else '',
            'listed_count': ('listed_count' in user_information) if user_information['listed_count'] else '',
            'favourites_count': ('favourites_count' in user_information) if user_information['favourites_count'] else '',
            'statuses_count': ('statuses_count' in user_information) if user_information['statuses_count'] else '',
        })
        RawFollowerDAO().delete_first({"_id": id_int})
        RawFollowerDAO().put(f)
        return
