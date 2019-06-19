class NoCooccurrenceGraphError(Exception):

    def __init__(self, start_date, end_date):
        self.message = f'No cooccurrence graph found for date window starting on {start_date} and finishing on {end_date}'

    def __str__(self):
        return self.message
