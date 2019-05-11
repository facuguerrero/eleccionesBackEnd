class NonExistentRawFollowerError(Exception):

    def __init__(self, follower_id):
        self.message = f"There is no raw follower with id '{follower_id}' in the database."

    def __str__(self):
        return self.message
