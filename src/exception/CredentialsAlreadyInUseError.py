class CredentialsAlreadyInUseError(Exception):

    def __init__(self, service_id):
        self.message = f'Service {service_id} is already using its credentials.'

    def __str__(self):
        return self.message
