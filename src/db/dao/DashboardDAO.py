from src.db.Mongo import Mongo
from src.db.dao.GenericDAO import GenericDAO
from src.util.DateUtils import DateUtils
from src.util.logging.Logger import Logger
from src.util.meta.Singleton import Singleton


class DashboardDAO(GenericDAO, metaclass=Singleton):

    def __init__(self):
        super(DashboardDAO, self).__init__(Mongo().get().db.dashboard)
        self.logger = Logger(self.__class__.__name__)

    def store(self, data):
        # Generate document
        data['date'] = DateUtils.today()
        # Store document
        self.collection.insert_one(data)
