from src.db.dao.CandidateDAO import CandidateDAO
from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
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


def update_tweets_user_id():
    tweets_to_update = RawTweetDAO().get_tweets_to_update(
        {'$or': [{'user_id': {'$type': 16}}, {'user_id': {'$type': 18}}]})
    for tweet_id, user_id in tweets_to_update.items():
        RawTweetDAO().upsert({'_id': tweet_id},
                             {'$set': {'user_id': str(user_id)}})