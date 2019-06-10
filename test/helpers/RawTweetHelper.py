from datetime import datetime


class RawTweetHelper:

    @staticmethod
    def common_raw_tweet():
        return {
            '_id': '1131254307045761029',
            'created_at': datetime.strptime('2019-05-22', '%Y-%m-%d'),
            'text': 'The #president should be our lord and saviour The #Emperor Alexander #Caniggia',
            'entities': {
                'hashtags': [
                    {'text': 'president'},
                    {'text': 'Emperor'},
                    {'text': 'Caniggia'}
                ],
            },
            'lang': 'es',
            'user_id': '4823354054'
        }

    @staticmethod
    def common_raw_tweet_one_hashtag():
        return {
            '_id': '1131254307045761029',
            'created_at': datetime.strptime('2019-05-22', '%Y-%m-%d'),
            'text': 'The president should be our lord and saviour The #Emperor Alexander Caniggia',
            'entities': {
                'hashtags': [
                    {'text': 'Emperor'}
                ],
            },
            'lang': 'es',
            'user_id': '4823354054'
        }

    @staticmethod
    def common_raw_tweet_ten_hashtags():
        return {
            '_id': '1131254307045761029',
            'created_at': datetime.strptime('2019-05-22', '%Y-%m-%d'),
            'text': 'A lot of hashtags',
            'entities': {
                'hashtags': [{'text': str(i)} for i in range(10)],
            },
            'lang': 'es',
            'user_id': '4823354054'
        }

    @staticmethod
    def common_raw_retweet():
        return {
            "_id": "1116319193450864640",
            "created_at": datetime.strptime('2019-05-22', '%Y-%m-%d'),
            "text": "RT @Fawaldman: @herlombardi bla bla bla bla...",
            "entities": {
                "hashtags": []
            },
            "retweeted_status": {
                "created_at": datetime.strptime('2019-05-22', '%Y-%m-%d'),
                "id": "1116176580869599232",
                "text": "@herlombardi bla bla bla bla...",
                "entities": {
                    "hashtags": []
                },
                "in_reply_to_user_id": 105180935,
            },
            "user_id": "4823354054"
        }
