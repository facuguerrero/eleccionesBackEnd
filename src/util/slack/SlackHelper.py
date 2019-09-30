import datetime

from slack import WebClient

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.util.EnvironmentUtils import EnvironmentUtils
from src.util.meta.Singleton import Singleton


class SlackHelper(metaclass=Singleton):

    web_client = WebClient('xoxb-624537791124-655373675696-ViP74LIGog1aXvG1OUPMhCEa')
    __env = None

    @classmethod
    def initialize(cls, env):
        cls.__env = env

    @classmethod
    def send_server_status(cls):
        return
        if not EnvironmentUtils.is_prod(cls.__env): return
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        followers_updated = RawFollowerDAO().get_users_updated_since_date(yesterday)
        tweets_updated = RawTweetDAO().get_count()  # new_followers = CandidatesFollowersDAO().get()

        message = f'Cantidad de tweets descargados hasta el momento: {tweets_updated} \n ' \
            f'Usuarios actualizados durante el día de ayer: {followers_updated} \n'
        cls.post_message_to_channel(message)

    @classmethod
    def post_message_to_channel(cls, message, channel="#reports"):
        return
        if not EnvironmentUtils.is_prod(cls.__env): return
        cls.web_client.api_call(
            api_method="chat.postMessage",
            json={'channel': channel,
                  'text': message
                  }
        )
