import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from src.util.meta.Singleton import Singleton
from src.util.scheduling.SchedulingExample import SchedulingExample


class Scheduler(metaclass=Singleton):

    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def set_up(self):
        self.scheduler.add_job(func=SchedulingExample.count, trigger='interval', seconds=10)
        self.scheduler.start()
        atexit.register(lambda: self.scheduler.shutdown())
