from datetime import datetime, timedelta
from src.exception.MissingConstructionParameterError import MissingConstructionParameterError


class Candidate:

    def __init__(self, **kwargs):
        self.nickname = kwargs.get('nickname', None)  # TODO: Check if this is useful or not
        self.screen_name = kwargs.get('screen_name', None)
        # By default, we will set 'yesterday' as last updating date
        self.last_updated_followers = kwargs.get('last_updated_followers', datetime.now() - timedelta(days=1))
        # Screen name is required
        if self.screen_name is None:
            raise MissingConstructionParameterError(self.__class__.__name__, 'screen_name')
