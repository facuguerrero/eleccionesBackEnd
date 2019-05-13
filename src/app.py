from flask import Flask
from flask_restful import Api

from src.api.CSVLoadingResource import CSVLoadingResource
from src.api.FollowerUpdatingResource import FollowerUpdatingResource
from src.api.PingResource import PingResource
from src.db.Mongo import Mongo
from src.db.db_initialization import create_indexes, create_base_entries
from src.util.logging.Logger import Logger
from src.util.scheduling.Scheduler import Scheduler

DBNAME = 'elections'
MONGO_URL = f'mongodb://localhost:27017/{DBNAME}'

app = Flask(__name__)
api = Api(app)
logger = Logger(__name__)

api.add_resource(PingResource, '/')
api.add_resource(CSVLoadingResource, '/csv/load')
api.add_resource(FollowerUpdatingResource, '/followers/update')


def set_up_context():
    app.config['MONGO_DBNAME'] = DBNAME
    app.config['MONGO_URI'] = MONGO_URL
    Mongo().db.init_app(app)
    with app.app_context():
        create_indexes()
        create_base_entries()


if __name__ == '__main__':
    set_up_context()
    Scheduler().set_up()
    app.run(host='0.0.0.0', port=8080, threaded=True)
