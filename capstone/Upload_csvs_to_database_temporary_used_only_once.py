import numpy as np
import pandas as pd
import ast

import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import *
from sklearn.preprocessing import MultiLabelBinarizer

from data_pull import upload_data_to_mysql,delete_data_from_mysql,retrieve_data_from_mysql

list_of_dates = ['2021-04-05','2021-04-12','2021-04-19','2021-04-26'
                ,'2021-05-03','2021-05-10','2021-05-17','2021-05-24'
                ,'2021-06-01','2021-06-07','2021-06-14','2021-06-21','2021-06-28'
                ,'2021-07-06','2021-07-12','2021-07-19','2021-07-26'
                ,'2021-08-02','2021-08-09','2021-08-16','2021-08-23', '2021-09-01'
]


delete_data_from_mysql()


for date in list_of_dates:

    chunk = pd.read_csv(f'spotify_{date}.csv')

    chunk = chunk[[
            'date of data pull', 'song_id', 'song name', 'album', 'artist', 'artist genres', 'artist popularity', 'artist number of followers'
            ,'artist type', 'album label', 'album popularity', 'song release date', 'length', 'song popularity'
            ,'key', 'mode', 'acousticness','valence', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness'
            ,'speechiness','tempo', 'time signature', 'tempo confidence', 'key confidence', 'time signature confidence'
            ,'mode confidence','rhythm version', 'synch version', 'number of segments', 'number of bars', 'number of beats'
            ,'number of sections', 'number of tatums' 
        ]]

    upload_data_to_mysql(chunk)


# main_counter = 1
# max_tries = 10


    # while main_counter <= max_tries:

    #     try:
    #         chunk2 = chunk[chunk.columns[:950]]
    #         upload_data_to_mysql(chunk2)
    #         counter = main_counter
    #         print(f'dataset uploaded successfully after {counter} attempts with {len(chunk2.columns)} columns')
    #         break

    #     except:
    #         main_counter+=1
    #         if main_counter>max_tries:
    #             print(f"aborted after trying {max_tries} times")
    #             break


