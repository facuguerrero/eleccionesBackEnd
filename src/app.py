# -*- coding: utf-8 -*-
from argparse import ArgumentParser

from flask import Flask
from flask_cors import CORS
from flask_restful import Api

from src.api.CSVLoadingResource import CSVLoadingResource
from src.api.CooccurrenceAnalysisResource import CooccurrenceAnalysisResource
from src.api.FollowerUpdatingResource import FollowerUpdatingResource
from src.api.HashtagUsageResource import HashtagUsageResource
from src.api.PingResource import PingResource
from src.api.TweetUpdatingResource import TweetUpdatingResource
from src.api.UserNetworkResource import UserNetworkResource
from src.db.Mongo import Mongo
from src.db.db_initialization import create_indexes, create_base_entries, create_queue_entries
from src.service.tweets.TweetUpdateServiceInitializer import TweetUpdateServiceInitializer
from src.service.user_network.UserNetworkRetrievalService import UserNetworkRetrievalService
from src.util.logging.Logger import Logger
from src.util.scheduling.Scheduler import Scheduler
from src.util.slack.SlackHelper import SlackHelper

DBNAME = 'elections'
AUTH = ''
ENV = 'dev'

app = Flask(__name__)
api = Api(app)
CORS(app)

# These are utility endpoints
api.add_resource(PingResource, '/')
api.add_resource(CSVLoadingResource, '/csv/load')
api.add_resource(FollowerUpdatingResource, '/followers/update')
api.add_resource(TweetUpdatingResource, '/tweets')
api.add_resource(CooccurrenceAnalysisResource, '/cooccurrence')
api.add_resource(UserNetworkResource, '/user_network')
api.add_resource(HashtagUsageResource, '/hashtag_usage')


def set_up_context(db_name, authorization, environment):
    # Configure logger
    Logger.set_up(environment)
    Logger(__name__).info(f'Starting application in environment {environment}')
    # Configure database
    app.config['MONGO_DBNAME'] = db_name
    app.config['MONGO_URI'] = f'mongodb://{authorization}localhost:27017/{db_name}'
    Mongo().db.init_app(app)
    SlackHelper.initialize(environment)
    with app.app_context():
        create_indexes()
        create_base_entries()
        create_queue_entries()


def init_services():
    # This is not necessary
    # UserTopicService().init_update_support_follower()
    TweetUpdateServiceInitializer().initialize_tweet_update_service()
    UserNetworkRetrievalService.start()


def parse_arguments():
    """ Read program arguments, which should be db_name and authentication data. The auth data is username:password. """
    parser = ArgumentParser()
    # Set up argument values
    parser.add_argument('--dbname', nargs='?', help='Name of the database to use')
    parser.add_argument('--auth', nargs='?', help='Database authentication data (username:password)')
    parser.add_argument('--env', nargs='?', help='Execution environment [dev; prod]')
    # Get program arguments
    arguments = parser.parse_args()
    db_name = DBNAME if not arguments.dbname else arguments.dbname
    db_auth = AUTH if not arguments.auth else f'{arguments.auth}@'
    environment = ENV if not arguments.env else arguments.env
    return db_name, db_auth, environment


if __name__ == '__main__':
    db, auth, env = parse_arguments()
    set_up_context(db, auth, env)
    Scheduler().set_up()
    init_services()
    app.run(port=8080, threaded=True)
