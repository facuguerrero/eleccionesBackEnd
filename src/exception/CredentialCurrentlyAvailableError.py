class CredentialCurrentlyAvailableError(Exception):

    def __init__(self, key):
        self.message = f'Credential with key {key} is currently available.'

    def __str__(self):
        return self.message
