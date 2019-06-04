class RawFollowerResponseMapper:

    @classmethod
    def map(cls, followers):
        """ Map a list of follower DTOs to JSON for response. """
        return [{'id': follower.id, 'follows': follower.follows, 'is_private': follower.is_private}
                for follower in followers]
