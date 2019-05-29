class NoMoreFollowersToUpdateTweetsError(Exception):

    def __init__(self):
        self.message = 'No more followers to update their tweets. '

    def __str__(self):
        return self.message
