from src.db.dao.HashtagDAO import HashtagDAO
from src.db.dao.RawTweetDAO import RawTweetDAO


class HashtagOriginService:

    @classmethod
    def process_tweet(cls, tweet):
        # Generate documents for hashtag origin collection and store
        for hashtag in {h['text'] for h in tweet['entities']['hashtags']}:
            # Make hashtag key
            key = hashtag.lower()
            # Lock for this hashtag to avoid undesired overwriting
            # ConcurrencyUtils().create_lock(key)
            # ConcurrencyUtils().acquire_lock(key)
            # Retrieve existing hashtag with that key
            document = HashtagDAO().find(key)
            # Only update tweet data if this tweet was older than the previous one
            if document is None or document['created_at'].timestamp() > tweet['created_at'].timestamp():
                # Store in database
                HashtagDAO().put(key, tweet, hashtag)
            else:
                # In this case we only add one to the number of appearances of the hashtag
                HashtagDAO().put(key, None, hashtag)
            # Unlock
            #ConcurrencyUtils().release_lock(key)
        # Mark tweet as already checked
        RawTweetDAO().hashtag_origin_checked(tweet)
