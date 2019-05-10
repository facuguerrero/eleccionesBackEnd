from flask_pymongo import PyMongo

from src.util.meta.Singleton import Singleton


class Mongo(metaclass=Singleton):

    def __init__(self):
        self.db = PyMongo()

    def get(self):
        return self.db
