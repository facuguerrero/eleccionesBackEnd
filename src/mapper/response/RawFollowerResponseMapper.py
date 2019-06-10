class RawFollowerResponseMapper:

    @classmethod
    def map(cls, followers):
        """ Map a list of follower DTOs to JSON for response. """
        return [{'id': follower.id, 'follows': follower.follows, 'is_private': follower.is_private,
                 'downloaded_on': follower.downloaded_on, 'location': follower.location,
                 'followers_count': follower.followers_count, 'friends_count': follower.friends_count,
                 'listed_count': follower.listed_count, 'favourites_count': follower.favourites_count,
                 'statuses_count': follower.statuses_count
                 }
                for follower in followers]
