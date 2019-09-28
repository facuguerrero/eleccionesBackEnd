from datetime import timedelta


class Similarities:

    def __init__(self, date):
        self.timestamp = date - timedelta(days=1)
        self.similarities = {}
        self.similarities_wor = None

    def add_similarity(self, key, value):
        self.similarities[key] = value

    def set_similarities_wor(self, swor):
        self.similarities_wor = swor
