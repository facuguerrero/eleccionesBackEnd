class NoAvailableCredentialsError(Exception):

    def __init__(self, service_id):
        self.message = f'All credentials are being used by service: {service_id}.'

    def __str__(self):
        return self.message
