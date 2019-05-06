from flask import Flask
from flask_restful import Api

from src.api.PingResource import PingResource
from src.db.database import mongo
from src.util.logging.Logger import Logger
from src.util.scheduling.Scheduler import Scheduler

DBNAME = 'elections'
MONGO_URL = f'mongodb://localhost:27017/{DBNAME}'

app = Flask(__name__)
api = Api(app)
logger = Logger(__name__)

api.add_resource(PingResource, '/')


def set_up_context():
    app.config['MONGO_DBNAME'] = DBNAME
    app.config['MONGO_URI'] = MONGO_URL
    mongo.init_app(app)


if __name__ == '__main__':
    set_up_context()
    Scheduler().set_up()
    app.run(host='0.0.0.0', port=8080, threaded=True)
