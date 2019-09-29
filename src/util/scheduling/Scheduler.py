import atexit

from apscheduler.schedulers.background import BackgroundScheduler

from src.service.dashboard.DashboardService import DashboardService
from src.service.followers.FollowerSupportService import FollowerSupportService
from src.service.followers.FollowerUpdateService import FollowerUpdateService
from src.service.hashtags.CooccurrenceAnalysisService import CooccurrenceAnalysisService
from src.service.queue_followers.FollowersQueueService import FollowersQueueService
from src.service.user_network.UserNetworkAnalysisService import UserNetworkAnalysisService
from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.meta.Singleton import Singleton
from src.util.slack.SlackHelper import SlackHelper


class Scheduler(metaclass=Singleton):

    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def set_up(self):
        """ Configure scheduler's jobs. """

        # Download new followers at 00:00:00 every day
        self.scheduler.add_job(func=FollowerUpdateService.update_followers, trigger='cron', hour=0, minute=0, second=0)

        # Adds all new followers
        self.scheduler.add_job(func=FollowersQueueService().add_last_downloaded_followers, trigger='cron', hour=4,
                               minute=0, second=0)

        # Adds not updated followers
        self.scheduler.add_job(func=FollowersQueueService().add_not_updated_followers, trigger='cron', hour=10,
                               minute=0, second=0)
        self.scheduler.add_job(func=FollowersQueueService().add_not_updated_followers, trigger='cron', hour=20,
                               minute=0, second=0)

        # Analyze cooccurrence at 00:01:00 every day
        self.scheduler.add_job(func=CooccurrenceAnalysisService.analyze, trigger='cron', hour=0, minute=1, second=0)

        # Send server status to Slack at 08:30:00 every day
        self.scheduler.add_job(func=SlackHelper.send_server_status, trigger='cron', hour=8, minute=30, second=0)

        # Update followers support
        self.scheduler.add_job(func=FollowerSupportService.init_update_support_follower, trigger='cron',
                               day_of_week='sun', hour=12, minute=30)
        self.scheduler.add_job(func=FollowerSupportService.init_update_support_follower, trigger='cron',
                               day_of_week='wed', hour=12, minute=30)
        # Add dashboard updating job
        update_hour = ConfigurationManager().get_int('dashboard_updating_time')
        self.scheduler.add_job(func=DashboardService.update_dashboard_data, trigger='cron', hour=update_hour)
        # Party relationship calculation
        self.scheduler.add_job(func=UserNetworkAnalysisService.calculate_relationships,
                               trigger='cron', hour=0, minute=0, second=0)
        # Start scheduler
        self.scheduler.start()
        atexit.register(lambda: self.scheduler.shutdown())
