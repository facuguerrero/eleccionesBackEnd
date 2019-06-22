from pymongo.errors import DuplicateKeyError

from src.db.dao.RawTweetDAO import RawTweetDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO
from src.util.logging.Logger import Logger


class UserHashtagService:

    @classmethod
    def insert_hashtags_of_already_downloaded_tweets(cls):
        """ This methods run over all downloaded tweets until 21/05 and insert every hash tag
        which appear in a specific user.
        """
        # TODO activate this
        # thread = Thread(target=cls.insert_hashtags)
        # thread.start()
        cls.insert_hashtags()

    @classmethod
    def insert_hashtags(cls):
        """ """
        tweets_cursor = RawTweetDAO().get_all({'in_user_hashtag_collection': {'$exists': False}})
        for tweet in tweets_cursor:
            cls.insert_hashtags_of_one_tweet(tweet)
            RawTweetDAO().update_first({'_id': tweet['_id']}, {'in_user_hashtag_collection': True})

    @classmethod
    def insert_hashtags_of_one_tweet(cls, tweet):
        """ create (user, hashtag, timestap) pairs from a given tweet. """
        # TODO Chequear en db:
        # db.raw_tweet.find({ 'entities': {$exists: False} }).count()
        # db.raw_tweet.find({ 'entities.hashtags': {$exists: False} }).count()
        user_hashtags = tweet['entities']['hashtags']
        user = tweet['user_id']
        for hashtag in user_hashtags:
            # guardar en la nueva db
            try:
                timestamp = tweet['created_at']
                hashtag_text = hashtag['text'].lower()
                UserHashtagDAO().insert({
                    '_id': user + hashtag + timestamp,
                    'user': user,
                    'hashtag': hashtag_text,
                    'timestamp': timestamp
                })
            except DuplicateKeyError:
                cls.get_logger().info(f'Trying to insert duplicated pair: {user} - {hashtag_text}')

    @classmethod
    def get_logger(cls):
        return Logger('TweetUpdateService')
