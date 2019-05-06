import json
from twython import Twython
import time
import numpy as np
import pandas as pd
from datetime import date

class FollowerUpdater:

    @classmethod
    def update_followers(cls):
        # !/usr/bin/env python
        # coding: utf-8

        # In[1]:

        # In[2]:

        # Credenciales para instanciar Twython

        print("Actualizacion de followers")

        with open("src/data/twitter_credentials.json", "r") as file:
            credentials = json.load(file)

        python_tweets = Twython(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])

        # In[ ]:

        # In[99]:

        def actualizar_followers(screen_name, apodo):

            # Lee la lista inicial de followers
            Followers_Candidate = pd.read_csv("src/data/" + apodo + "_followers.csv")
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
            DF.to_csv("src/data/" + apodo + "_followers.csv", header=True, index=False, mode='a')

        # In[ ]:

        # In[98]:

        # screen_name="mauriciomacri"
        # apodo = "macri"
        # actualizar_followers(screen_name, apodo)

        # In[85]:

        # screen_name="sergiomassa"
        # apodo = "massa"
        # actualizar_followers(screen_name, apodo)

        # In[84]:

        # screen_name="rlavagna"
        # apodo = "lavagna"
        # actualizar_followers(screen_name, apodo)

        # In[82]:

        # screen_name = "urtubeyjm"
        # apodo = "urtubey"
        # actualizar_followers(screen_name, apodo)

        # In[83]:

        # screen_name="jlespert"
        # apodo = "espert"
        # actualizar_followers(screen_name, apodo)

        # In[100]:

        # screen_name = "CFKArgentina"
        # apodo = "cfk"
        # actualizar_followers(screen_name, apodo)

        # In[ ]:

        # ### Script para actualizar los seguidores de un candidato a diario

        # In[89]:

        lista_candidatos = [
            #("mauriciomacri", "macri"),
            #("sergiomassa", "massa"),
            #("rlavagna", "lavagna"),
            ("urtubeyjm", "urtubey"),
            #("jlespert", "espert"),
            #("CFKArgentina", "cfk")
        ]

        # In[97]:

        for (screen_name, apodo) in lista_candidatos:
            print("Actualizando seguidores de " + screen_name + "... ", end='')
            actualizar_followers(screen_name, apodo)
            print(" [OK]")

        # In[ ]:

        # #### ¿Cuántos usuarios totales tenemos hasta el momento?

        # In[108]:

        # Cantidad de usuarios que tenemos hasta el momento
        lista_listas_followers = list()
        for (screen_name, apodo) in lista_candidatos:
            Followers_Candidate = pd.read_csv("src/data/" + apodo + "_followers.csv")
            lista_listas_followers.append(Followers_Candidate.user_id)

        len(set(np.concatenate(lista_listas_followers)))

        # In[109]:

        len(np.concatenate(lista_listas_followers))

        # In[ ]:



