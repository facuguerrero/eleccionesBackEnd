from configparser import ConfigParser
from os.path import abspath, join, dirname

from src.util.meta.Singleton import Singleton


class ConfigurationManager(metaclass=Singleton):

    CONFIG_PATH = f"{abspath(join(dirname(__file__), '../../'))}/resources/config/properties.cfg"

    def __init__(self):
        self.parser = ConfigParser()
        with open(ConfigurationManager.CONFIG_PATH, 'r') as fd:
            self.parser.read_file(fd)

    def get_int(self, config_key):
        """ Get value associated to key as integer. """
        return int(self.get(config_key))

    def get_float(self, config_key):
        """ Get value associated to key as integer. """
        return float(self.get(config_key))

    def get_string(self, config_key):
        """ Get value associated to key as string. """
        return self.get(config_key)

    def get_boolean(self, config_key):
        return self.parser.getboolean(config_key, 'true')

    def get(self, config_key):
        """ Get value associated to key. """
        return self.parser.get('default', config_key)
