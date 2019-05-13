import atexit
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.service.followers.FollowerUpdateService import FollowerUpdateService
from src.util.meta.Singleton import Singleton


class Scheduler(metaclass=Singleton):

    def __init__(self):
        self.scheduler = AsyncIOScheduler()

    def set_up(self):
        """ Configure scheduler's jobs. """
        # Execute at 00:00:00 every day
        self.scheduler.add_job(func=FollowerUpdateService.update_followers, trigger='cron', hour=0, minute=0, second=0)
        self.scheduler.start()
        atexit.register(lambda: self.scheduler.shutdown())
