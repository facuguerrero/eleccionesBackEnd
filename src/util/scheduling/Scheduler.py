import atexit

from apscheduler.schedulers.background import BackgroundScheduler

from src.service.followers.FollowerUpdateService import FollowerUpdateService
from src.util.meta.Singleton import Singleton
from src.util.slack.SlackHelper import SlackHelper


class Scheduler(metaclass=Singleton):

    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def set_up(self):
        """ Configure scheduler's jobs. """
        # Execute at 00:00:00 every day
        self.scheduler.add_job(func=FollowerUpdateService.update_followers, trigger='cron', hour=0, minute=0, second=0)
        # TODO: Activate this
        # self.scheduler.add_job(func=CooccurrenceAnalysisService.run_analysis, trigger='cron', hour=0, minute=30, second=0)
        # self.scheduler.add_job(func=FollowersQueueService.add_last_downloaded_followers, trigger='cron', hour=19, minute=1, second=0)
        self.scheduler.add_job(func=SlackHelper.send_server_status, trigger='cron', hour=8, minute=30, second=0)
        self.scheduler.start()
        atexit.register(lambda: self.scheduler.shutdown())
