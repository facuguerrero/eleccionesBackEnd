class RawFollowerHelper:

    @classmethod
    def create_many_followers_ids(cls, _, __):
        followers = []
        for i in range(20):
            followers.append({'_id': str(i)})
        return followers
