from src.db.dao.CooccurrenceDAO import CooccurrenceDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.util.logging.Logger import Logger


class HashtagCooccurrenceService:

    @classmethod
    def process_tweet(cls, tweet):
        """ Process tweet for hashtag cooccurrence detection. """
        if cls.__is_processable(tweet):
            # Flatten list of hashtags and keep distinct values only
            hashtags = list({h['text'].lower() for h in tweet['entities']['hashtags']})
            # Generate documents for cooccurrence collection and store
            for i in range(len(hashtags) - 1):
                for j in range(i + 1, len(hashtags)):
                    # Store in database
                    CooccurrenceDAO().store(tweet, sorted([hashtags[i], hashtags[j]]))
        # Mark tweet as already used
        RawTweetDAO().cooccurrence_checked(tweet)

    @classmethod
    def __is_processable(cls, tweet):
        """ Verify if this tweet has the characteristics to bo analyzed for hashtag cooccurrence.
        Cooccurrence calculation is only possible if it is not a retweet and has at least 2 hashtags.
        The upper bound is arbitrary."""
        return not tweet.get('retweeted_status', None) and 1 < len(tweet['entities']['hashtags']) < 8

    @classmethod
    def get_logger(cls):
        return Logger('HashtagCooccurrenceService')
