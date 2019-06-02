from src.model.Credential import Credential
from src.util.twitter.TwitterUtils import TwitterUtils
from test.meta.CustomTestCase import CustomTestCase


class TestTwitterUtils(CustomTestCase):

    def test_twython_instance_creation_consumer_data(self):
        credential = Credential(**{'ID': 'test', 'CONSUMER_KEY': 'test', 'CONSUMER_SECRET': 'test'})
        twitter = TwitterUtils.twitter(credential)
        assert twitter.app_key is not None
        assert twitter.app_secret is not None
        assert twitter.oauth_token is None
        assert twitter.oauth_token_secret is None

    def test_twython_instance_creation_oauth_data(self):
        credential = Credential(**{'ID': 'test', 'ACCESS_TOKEN': 'test', 'ACCESS_SECRET': 'test'})
        twitter = TwitterUtils.twitter(credential)
        assert twitter.app_key is None
        assert twitter.app_secret is None
        assert twitter.oauth_token is not None
        assert twitter.oauth_token_secret is not None

    def test_twython_instance_creation_all_fields(self):
        credential = Credential(**{'ID': 'test', 'ACCESS_TOKEN': 'test', 'ACCESS_SECRET': 'test',
                                   'CONSUMER_KEY': 'test', 'CONSUMER_SECRET': 'test'})
        twitter = TwitterUtils.twitter(credential)
        assert twitter.app_key is not None
        assert twitter.app_secret is not None
        assert twitter.oauth_token is not None
        assert twitter.oauth_token_secret is not None
