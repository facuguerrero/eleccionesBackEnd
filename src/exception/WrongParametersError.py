class WrongParametersError(Exception):

    def __init__(self, field_name):
        self.message = f'Field {field_name} has an incorrect value.'

    def __str__(self):
        return self.message
