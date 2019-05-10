from src.exception.MissingConstructionParameterError import MissingConstructionParameterError


class Credential:

    def __init__(self, **kwargs):
        # ID is required
        self.id = kwargs.get('ID', None)
        if self.id is None:
            raise MissingConstructionParameterError(self.__class__.__name__, 'ID')
        # At least one pair should be present
        self.consumer_key = kwargs.get('CONSUMER_KEY', None)
        self.consumer_secret = kwargs.get('CONSUMER_SECRET', None)
        self.access_token = kwargs.get('ACCESS_TOKEN', None)
        self.access_secret = kwargs.get('ACCESS_SECRET', None)
        if (self.consumer_key is None or self.consumer_secret is None) and \
                (self.access_token is None or self.access_secret is None):
            raise MissingConstructionParameterError(self.__class__.__name__, 'Key Pair')
