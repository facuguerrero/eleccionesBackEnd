import json
from os.path import abspath, join, dirname


class JsonLoader:

    RESOURCES_PATH = abspath(join(dirname(__file__), '../resources'))

    @classmethod
    def json_from_string_resource(cls, path):
        with open(f'{cls.RESOURCES_PATH}/{path}.json') as fd:
            return json.load(fd)
