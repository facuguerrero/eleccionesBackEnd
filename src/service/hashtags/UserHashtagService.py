from threading import Thread

from src.db.dao.RawTweetDAO import RawTweetDAO
from src.db.dao.UserHashtagDAO import UserHashtagDAO
from src.util.logging.Logger import Logger


class UserHashtagService:

    @classmethod
    def insert_hashtags_of_already_downloaded_tweets(cls):
        """ This methods run over all downloaded tweets until 21/05 and insert every hash tag
        which appear in a specific user.
        """
        thread = Thread(target=cls.insert_hashtags)
        thread.start()

    @classmethod
    def insert_hashtags(cls):
        """ """
        cls.get_logger().info("Starting User Hashtag process.")
        tweets_cursor = RawTweetDAO().get_all({"in_user_hashtag_collection": {'$exists': False}})
        for tweet in tweets_cursor:
            cls.insert_hashtags_of_one_tweet(tweet)
            RawTweetDAO().update_first({'_id': tweet['_id']}, {'in_user_hashtag_collection': True})
        cls.get_logger().info("User Hashtag Service finished.")

    @classmethod
    def insert_hashtags_of_one_tweet(cls, tweet):
        """ create (user, hashtag, timestap) pairs from a given tweet. """
        user_hashtags = tweet['entities']['hashtags']
        user = tweet['user_id']
        for hashtag in user_hashtags:
            timestamp = tweet['created_at']
            hashtag_text = hashtag['text'].lower()
            UserHashtagDAO().insert({
                'user': user,
                'hashtag': hashtag_text,
                'timestamp': timestamp
            })

    @classmethod
    def get_logger(cls):
        return Logger('TweetUpdateService')
