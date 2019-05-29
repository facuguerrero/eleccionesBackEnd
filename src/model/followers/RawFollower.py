from src.exception.MissingConstructionParameterError import MissingConstructionParameterError


class RawFollower:

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        if self.id is None:
            raise MissingConstructionParameterError(self.__class__.__name__, 'id')
        self.downloaded_on = kwargs.get('downloaded_on', None)
        self.follows = kwargs.get('follows', None)
        self.is_private = kwargs.get('is_private', False)
        self.location = kwargs.get('location', None)
        self.followers_count = kwargs.get('followers_count', None)
        self.friends_count = kwargs.get('friends_count', None)
        self.listed_count = kwargs.get('listed_count', None)
        self.favourites_count = kwargs.get('favourites_count', None)
        self.statuses_count = kwargs.get('statuses_count', None)