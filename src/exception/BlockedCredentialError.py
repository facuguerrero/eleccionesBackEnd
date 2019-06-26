class BlockedCredentialError(Exception):

    def __init__(self):
        self.message = f'Credential seems to be blocked.'

    def __str__(self):
        return self.message
