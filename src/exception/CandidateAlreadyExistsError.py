class CandidateAlreadyExistsError(Exception):

    def __init__(self, screen_name):
        self.message = f'Candidate with name {screen_name} already exists in database.'

    def __str__(self):
        return self.message
