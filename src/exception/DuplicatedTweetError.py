class DuplicatedTweetError(Exception):

    def __init__(self):
        self.message = 'Trying to insert an existing tweet'

    def __str__(self):
        return self.message
