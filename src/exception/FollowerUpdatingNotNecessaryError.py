class FollowerUpdatingNotNecessaryError(Exception):

    def __init__(self):
        self.message = 'Followers have been updated for every candidate.'

    def __str__(self):
        return self.message
