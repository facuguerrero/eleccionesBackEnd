class NonExistentDataForMatrixError(Exception):

    def __init__(self, matrix):
        self.message = f"There is no data for '{matrix}'"
        self.matrix = matrix

    def __str__(self):
        return self.message
