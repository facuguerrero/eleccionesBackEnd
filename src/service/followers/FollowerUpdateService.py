import time
import numpy as np
import pandas as pd
from twython import Twython
from datetime import date

from src.service.candidates.CandidateService import CandidateService
from src.service.credentials.CredentialService import CredentialService
from src.util.logging.Logger import Logger


class FollowerUpdateService:

    LOGGER = Logger(__name__)

    """
    Create a polling system that locks credentials until all requests for a given candidate are made. Credentials
    should be locked by kind of request (say, by service). For example, one credential should be available at the same
    time for Tweet retrieval and Follower retrieval.
    We have to poll candidates and mark them as used for the day. We could put an 'updated' field and check before
    returning it. When all were used, the process ends.
    We have to wait until some credential is free, supposing we still have candidates to work with.
    Each active credential will work on a different thread, we should check how to join and make sure we are done.
    This same logic can be replicated for all services.
    """

    @classmethod
    def update_followers(cls):
        cls.LOGGER.info("Follower updating process started.")
        # Create Twython instance with given credentials
        credentials = CredentialService().get_credentials()
        python_tweets = Twython(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
        # Retrieve candidates
        candidates = CandidateService.get_candidates()

        for (screen_name, nickname) in candidates:
            cls.LOGGER.info(f"Started updating {screen_name}'s followers.")
            cls.update_followers_for(python_tweets, screen_name, nickname)
            cls.LOGGER.info(f"Finished updating {screen_name}'s followers.")

        # Cantidad de usuarios que tenemos hasta el momento
        lista_listas_followers = list()
        for (screen_name, nickname) in candidates:
            Followers_Candidate = pd.read_csv("src/data/" + nickname + "_followers.csv")
            lista_listas_followers.append(Followers_Candidate.user_id)

        len(set(np.concatenate(lista_listas_followers)))

        len(np.concatenate(lista_listas_followers))

    @classmethod
    def update_followers_for(cls, python_tweets, screen_name, nickname):

        # Lee la lista inicial de followers
        Followers_Candidate = pd.read_csv("src/data/" + nickname + "_followers.csv")
        t0_followers_list = set(Followers_Candidate.user_id)

        followers_usuario = python_tweets.get_followers_ids(screen_name=screen_name)

        T = len(followers_usuario['ids'])

        lista_listas_followers = list()
        lista_listas_followers.append(followers_usuario['ids'])

        # Para asegurarnos de que no hay más seguidores nuevos, esperaremos hasta encontrar 100 repetidos
        overlaps = len(set(followers_usuario['ids']).intersection(t0_followers_list))

        # Mientras no se acaben los seguidores, extraemos otro grupo. Cuando encontremos que los resultados
        # se parecen mucho a lo que ya tenemos, paramos
        while (followers_usuario["next_cursor"] != -1) & (followers_usuario["next_cursor"] != 0) & (overlaps < 100):
            try:
                followers_usuario = python_tweets.get_followers_ids(screen_name=screen_name,
                                                                    cursor=followers_usuario["next_cursor_str"])
                lista_listas_followers.append(followers_usuario['ids'])
                T = T + len(followers_usuario['ids'])
                # print("Evolución: " + str(T) + " followers descargados")
                overlaps = len(set(followers_usuario['ids']).intersection(t0_followers_list))
            except:
                pass
            time.sleep(60)

        # Aquí tenemos ya a todos los nuevos
        new_followers_usuario = set(np.concatenate(lista_listas_followers)) - t0_followers_list
        print(len(new_followers_usuario), end='')

        # Enviamos los nuevos datos a un archivo de texto
        DF = pd.DataFrame(new_followers_usuario, columns=["user_id"])
        DF["link_date"] = date.today()

        # Hacemos append de los nuevos seguidores con la fecha de hoy
        DF.to_csv("src/data/" + nickname + "_followers.csv", header=True, index=False, mode='a')
