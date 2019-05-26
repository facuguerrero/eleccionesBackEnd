from src.exception.MissingConstructionParameterError import MissingConstructionParameterError


class RawTweet:

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        if self.id is None:
            raise MissingConstructionParameterError(self.__class__.__name__, 'id')
        self.created_at = kwargs.get('created_at', None)
        self.text = kwargs.get('text', None)
        self.user_id = kwargs.get('user_id', False)
