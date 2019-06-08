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


def update_user_id():
    tweets_to_update = RawFollowerDAO().get_users_to_update(
        {'$or': [{'_id': {'$type': 16}}, {'_id': {'$type': 18}}]})
    for document in tweets_to_update:
        RawFollowerDAO().insert({'id': str(document['id']),
                                 'follows': document['follows'],
                                 'downloaded_on': document['downloaded_on'],
                                 'is_private': document['is_private'],
                                 'location': get_if_value(document, 'location'),
                                 'followers_count': get_if_value(document, 'followers_count'),
                                 'friends_count': get_if_value(document, 'friends_count'),
                                 'listed_count': get_if_value(document, 'listed_count'),
                                 'favourites_count': get_if_value(document, 'favourites_count'),
                                 'statuses_count': get_if_value(document, 'statuses_count')
                                 })
        RawFollowerDAO().remove_document({'_id': document['id']})


def get_if_value(document, key):
    if key in document:
        return document[key]
    return None
