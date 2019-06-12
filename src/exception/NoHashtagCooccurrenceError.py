class NoHashtagCooccurrenceError(Exception):

    def __init__(self, start_date, end_date):
        self.message = f'There are no documents for hashtag cooccurrence in time window starting on' \
                       f' {start_date.date()}' \
                       f' and ending on {end_date.date()}.'

    def __str__(self):
        return self.message
