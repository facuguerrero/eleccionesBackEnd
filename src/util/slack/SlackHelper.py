import datetime

from slack import WebClient

from src.db.dao.RawFollowerDAO import RawFollowerDAO
from src.db.dao.RawTweetDAO import RawTweetDAO
from src.util.meta.Singleton import Singleton


class SlackHelper(metaclass=Singleton):
    web_client = WebClient('xoxb-624537791124-655373675696-ViP74LIGog1aXvG1OUPMhCEa')

    @classmethod
    def send_server_status(cls):
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        followers_updated = RawFollowerDAO().get_users_updated_since_date(yesterday)
        tweets_updated = RawTweetDAO().get_count()
        init_date = datetime.datetime(2019, 5, 20, 0, 0, 0)
        total_followers_updated = RawFollowerDAO().get_users_updated_since_date(init_date)
        # new_followers = CandidatesFollowersDAO().get()

        message = f'Cantidad de tweets descargados hasta el momento: {tweets_updated} \n ' \
            f'Usuarios actualizados durante el día de ayer: {followers_updated} \n' \
            f'Cantidad de usuarios totales actualizados: {total_followers_updated}'
        cls.post_message_to_channel(message)

    @classmethod
    def post_message_to_channel(cls, message, channel="#reports"):
        cls.web_client.api_call(
            api_method="chat.postMessage",
            json={'channel': channel,
                  'text': message
                  }
        )
