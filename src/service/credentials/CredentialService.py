import json
from os.path import abspath, join, dirname

from src.exception.CredentialCurrentlyAvailableError import CredentialCurrentlyAvailableError
from src.exception.CredentialsAlreadyInUseError import CredentialsAlreadyInUseError
from src.exception.NoAvailableCredentialsError import NoAvailableCredentialsError
from src.model.Credential import Credential
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class CredentialService(metaclass=Singleton):

    CREDENTIALS_PATH = f"{abspath(join(dirname(__file__), '../../..'))}/twitter_credentials.json"

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.in_use = set()
        self.credentials = []
        # Load credentials file and create objects to access their elements
        try:
            with open(CredentialService.CREDENTIALS_PATH, 'r') as file:
                loaded = json.load(file)
                for value in loaded:
                    self.credentials.append(Credential(**value))
        except IOError:
            self.logger.error('Credentials file do not found')

    def get_all_credentials_for_service(self, service_id):
        """ Return all credentials for a given service. """
        self.logger.info(f'Returning all credentials for service {service_id}.')
        # Check if some credential has already been assigned
        for credential in self.credentials:
            if f"{credential.id}-{service_id}" in self.in_use:
                raise CredentialsAlreadyInUseError(service_id)
        self.logger.info('Checked credentials')
        # Store in the in use set. We iterate twice because the number of credentials is small and it is easier than
        # doing rollbacks with the already stored credentials if we need to raise an exception
        for credential in self.credentials:
            self.in_use.add(f"{credential.id}-{service_id}")

        self.logger.info(self.credentials)

        return self.credentials

    def get_credential_for_service(self, service_id):
        """ Get credential if current service is not using all of the available credentials. """
        for credential in self.credentials:
            if f"{credential.id}-{service_id}" not in self.in_use:
                self.logger.info(f'Returning credential {credential.id} for service {service_id}.')
                self.in_use.add(f"{credential.id}-{service_id}")
                return credential
        raise NoAvailableCredentialsError(service_id)

    def unlock_credential(self, credential_id, service_id):
        """ Unlock credential for a given service. """
        key = f'{credential_id}-{service_id}'
        if key not in self.in_use:
            raise CredentialCurrentlyAvailableError(key)
        self.logger.info(f'Unlocking credential {credential_id} for service {service_id}.')
        self.in_use.remove(key)
