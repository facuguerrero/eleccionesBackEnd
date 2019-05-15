import sys

from flask import Flask
from flask_restful import Api

from src.api.CSVLoadingResource import CSVLoadingResource
from src.api.FollowerUpdatingResource import FollowerUpdatingResource
from src.api.PingResource import PingResource
from src.db.Mongo import Mongo
from src.db.db_initialization import create_indexes, create_base_entries
from src.util.ContextInitializer import ContextInitializer
from src.util.logging.Logger import Logger
from src.util.scheduling.Scheduler import Scheduler

DBNAME = 'elections'
AUTH = ''

app = Flask(__name__)
api = Api(app)
logger = Logger(__name__)

api.add_resource(PingResource, '/')
api.add_resource(CSVLoadingResource, '/csv/load')
api.add_resource(FollowerUpdatingResource, '/followers/update')


def set_up_context(db_name, authorization):
    app.config['MONGO_DBNAME'] = db_name
    app.config['MONGO_URI'] = f'mongodb://{authorization}localhost:27017/{db_name}'
    Mongo().db.init_app(app)
    ContextInitializer.initialize_context()
    with app.app_context():
        create_indexes()
        create_base_entries()


def parse_arguments():
    """ Read program arguments, which should be db_name and authentication data. The auth data is username:password. """
    if len(sys.argv) < 3:
        logger.info('Using default database parameters')
        return DBNAME, AUTH
    else:
        logger.info('Using non-default database parameters.')
        return sys.argv[1], f'{sys.argv[2]}@'


if __name__ == '__main__':
    dbname, auth = parse_arguments()
    set_up_context(dbname, auth)
    Scheduler().set_up()
    app.run(port=8080, threaded=True)
