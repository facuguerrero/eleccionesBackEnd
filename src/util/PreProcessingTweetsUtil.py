import csv
import pickle
import datetime
from os.path import join, abspath, dirname

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.exception.DuplicatedTweetError import DuplicatedTweetError
from src.exception.NonExistentRawFollowerError import NonExistentRawFollowerError
from src.model.followers.RawFollower import RawFollower
from src.util.logging.Logger import Logger


class PreProcessingTweetsUtil:
    DATE_FORMAT = '%Y-%m-%d'
    FOLLOWERS_PATH_FORMAT = f"{abspath(join(dirname(__file__), '../../../'))}/elecciones/data/"

    @classmethod
    def load_followers(cls):
        with open('PUT PATH HERE', 'r') as fd:
            reader = csv.reader(fd, delimiter=',')
            for row in reader:
                follower = RawFollower(**{'_id': row[0],
                                          'downloaded_on': datetime.datetime.strptime(row[1],
                                                                                      PreProcessingTweetsUtil.DATE_FORMAT),
                                          'follows': 'prueba'})
                RawFollowerDAO().put(follower)

    @classmethod
    def load_tweets(cls):
        cls.get_logger().info(f'Inserting in DB pre download tweets ')
        candidates = ["cfk", "macri"]
        min_tweet_date = datetime.datetime(2019, 1, 1).astimezone(
            pytz.timezone('America/Argentina/Buenos_Aires')) - datetime.timedelta()
        tweets_updated = 0
        for candidate in candidates:
            path = cls.FOLLOWERS_PATH_FORMAT + candidate + ".pickle"
            download_tweets = {}
            try:
                with open(path, 'rb') as frb:
                    download_tweets = pickle.load(frb)
                frb.close()
            except IOError:
                cls.get_logger().error('Error opening the file')
            cls.get_logger().info(f'Inserting in db {candidate}\'s followers tweets.')
            cls.get_logger().info(str(download_tweets.keys()))
            for follower, follower_tweets in download_tweets.items():
                if len(follower_tweets) != 0:
                    cls.update_follower_with_first_tweet(follower, follower_tweets[0])
                    for tweet in follower_tweets:
                        tweet_date = cls.get_formatted_date(tweet['created_at'])
                        if tweet_date >= min_tweet_date:
                            # Clean tweet's information
                            tweet["_id"] = tweet['id_str']
                            tweet.pop('id')
                            tweet.pop('id_str')
                            tweet['created_at'] = tweet_date
                            tweet['user_id'] = tweet['user']['id']
                            tweet.pop('user')
                            try:
                                RawTweetDAO().insert_tweet(tweet)
                            except DuplicatedTweetError:
                                break
                            tweets_updated += 1
                    cls.get_logger().info(f'{follower} updated')
            cls.get_logger().info(f'Tweets updated: {str(tweets_updated)}')

    @classmethod
    def get_formatted_date(cls, date):
        return datetime.datetime.strptime(date, '%a %b %d %H:%M:%S %z %Y').astimezone(
            pytz.timezone('America/Argentina/Buenos_Aires'))

    @classmethod
    def update_follower_with_first_tweet(cls, follower, tweet):
        try:
            follower_result = RawFollowerDAO().get(follower)
            today = datetime.datetime.today()
            user_information = tweet['user']
            updated_raw_follower = RawFollower(**{'id': follower,
                                                  'follows': follower_result.follows,
                                                  'downloaded_on': today,
                                                  'location': user_information['location'],
                                                  'followers_count': user_information['followers_count'],
                                                  'friends_count': user_information['friends_count'],
                                                  'listed_count': user_information['listed_count'],
                                                  'favourites_count': user_information['favourites_count'],
                                                  'statuses_count': user_information['statuses_count']
                                                  })
            RawFollowerDAO().put(updated_raw_follower)
        except NonExistentRawFollowerError:
            cls.get_logger().error(f'Follower {follower} does not exists')

    @classmethod
    def fix_followers_update(cls):
        followers = RawFollowerDAO().get_all({'downloaded_on': {'$gt': datetime.datetime(2019, 5, 29, 0, 0, 0)}})
        for follower in followers:
            real_follows = []
            for seguido in follower['follows']:
                if isinstance(seguido, str):
                    real_follows.append(seguido)
            RawFollowerDAO().update_follows(follower['_id'], real_follows)

    @classmethod
    def get_logger(cls):
        return Logger('CSVUtils')
