class Similarities:

    def __init__(self, date):
        self.timestamp = date
        self.similarities = {}

    def add_similarity(self, key, value):
        self.similarities[key] = value
