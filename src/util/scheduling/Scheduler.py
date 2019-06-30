import atexit

from apscheduler.schedulers.background import BackgroundScheduler

from src.service.followers.FollowerUpdateService import FollowerUpdateService
from src.service.hashtags.CooccurrenceAnalysisService import CooccurrenceAnalysisService
from src.service.tweets.FollowersQueueService import FollowersQueueService
from src.service.tweets.TweetUpdateServiceInitializer import TweetUpdateServiceInitializer
from src.util.meta.Singleton import Singleton
from src.util.slack.SlackHelper import SlackHelper


class Scheduler(metaclass=Singleton):

    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def set_up(self):
        """ Configure scheduler's jobs. """
        # Download new followers at 00:00:00 every day
        self.scheduler.add_job(func=FollowerUpdateService.update_followers, trigger='cron', hour=0, minute=0, second=0)
        self.scheduler.add_job(func=FollowersQueueService().add_last_downloaded_followers, trigger='cron', hour=3,
                               minute=0, second=0)
        # Analyze cooccurrence at 00:30:00 every day
        self.scheduler.add_job(func=CooccurrenceAnalysisService.analyze(), trigger='cron', hour=0, minute=30, second=0)
        # Send server status to Slack at 08:30:00 every day
        self.scheduler.add_job(func=SlackHelper.send_server_status, trigger='cron', hour=8, minute=30, second=0)
        self.scheduler.start()
        atexit.register(lambda: self.scheduler.shutdown())

    def add_job_to_restart_credential(self, credential_id, h, m, s):
        # Habria que ejecutarlo una única vez
        self.scheduler.add_job(func=TweetUpdateServiceInitializer().restart_credential, trigger='cron', hour=h,
                               minute=m, second=s)
