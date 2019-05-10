class CandidateCurrentlyAvailableForUpdateError(Exception):

    def __init__(self, screen_name):
        self.message = f'Candidate {screen_name} is currently available for updating.'

    def __str__(self):
        return self.message
