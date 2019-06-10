from src.db.dao.HashtagDAO import HashtagDAO
from src.db.dao.RawTweetDAO import RawTweetDAO


class HashtagOriginService:

    @classmethod
    def process_tweet(cls, tweet):
        # Generate documents for hashtag origin collection and store
        for hashtag in {h['text'] for h in tweet['entities']['hashtags']}:
            # Make hashtag key
            key = hashtag.lower()
            # Retrieve existing hashtag with that key
            document = HashtagDAO().find(key)
            # Only update tweet data if this tweet was older than the previous one
            if document is None or document['created_at'] > tweet['created_at']:
                # Store in database
                HashtagDAO().put(key, tweet, hashtag)
        # Mark tweet as already checked
        RawTweetDAO().hashtag_origin_checked(tweet)
