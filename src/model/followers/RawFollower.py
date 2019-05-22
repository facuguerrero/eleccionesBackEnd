from src.exception.MissingConstructionParameterError import MissingConstructionParameterError


class RawFollower:

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        if self.id is None:
            raise MissingConstructionParameterError(self.__class__.__name__, 'id')
        self.downloaded_on = kwargs.get('downloaded_on', None)
        self.follows = kwargs.get('follows', None)
        self.is_private = kwargs.get('is_private', False)
