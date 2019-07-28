class HashtagUsageResponseMapper:

    @classmethod
    def map_one(cls, document):
        """ Map datetime fields for response. """
        document['date_axis'] = [time.timestamp() for time in document['date_axis']]
        return document
