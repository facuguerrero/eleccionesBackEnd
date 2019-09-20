from twython import Twython


class   TwitterUtils:

    @classmethod
    def twitter(cls, credential):
        """ Create Twython instance depending on credential data. """
        if credential.access_token is None:
            return cls.twitter_with_app_auth(credential)
        elif credential.consumer_key is None:
            return cls.twitter_with_oauth(credential)
        else:
            twitter = Twython(app_key=credential.consumer_key, app_secret=credential.consumer_secret,
                              oauth_token=credential.access_token, oauth_token_secret=credential.access_secret)
        return twitter

    @classmethod
    def twitter_with_app_auth(cls, credential):
        """ Create Twython instance with app key and secret. """
        return Twython(app_key=credential.consumer_key, app_secret=credential.consumer_secret)

    @classmethod
    def twitter_with_oauth(cls, credential):
        """ Create Twython instance with oauth token and secret. """
        return Twython(oauth_token=credential.access_token, oauth_token_secret=credential.access_secret)
