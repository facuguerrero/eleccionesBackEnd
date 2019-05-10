class MissingConstructionParameterError(Exception):

    def __init__(self, klazz, field):
        self.message = f'Required field {field} missing when building instance of class {klazz}.'

    def __str__(self):
        return self.message
