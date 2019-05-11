class NonExistentCandidateError(Exception):

    def __init__(self, screen_name):
        self.message = f"There is no candidate with screen name '{screen_name}' in the database."

    def __str__(self):
        return self.message
