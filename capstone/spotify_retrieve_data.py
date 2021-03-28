import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time 
import requests
import json
import openpyxl
import logging

from sklearn.preprocessing import MultiLabelBinarizer

from datetime import datetime
from decouple import config

from functions_for_spotify_data_retrieval import *


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

client_id = config('spotify_client_id')
client_secret = config('spotify_client_secret')

current_year = pd.to_datetime("today").year

year_list = list(np.arange(current_year-20,current_year+1))

track_ids = []

for year in year_list:
    holder_list = []
    holder_list = retrieve_track_ids(year)
    track_ids.extend(holder_list)

logging.debug(f'Extracted the song IDs of {len(track_ids)} songs')


# loop over track ids 
tracks = []
for i in range(len(track_ids)):
#     time.sleep(.5)
    try:
        track = getTrackFeatures(track_ids[i])
        tracks.append(track)
    except:
        continue

        
# create dataset
df = pd.DataFrame(tracks, columns = [
    'song_id', 'song name', 'album', 'artist', 'artist genres', 'artist popularity', 'artist number of followers'
    ,'artist type', 'album label', 'album popularity', 'song release date', 'length', 'song popularity'
    ,'key', 'mode', 'acousticness','valence', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness'
    ,'speechiness','tempo', 'time signature', 'tempo confidence', 'key confidence', 'time signature confidence'
    ,'mode confidence','rhythm version', 'synch version', 'number of segments', 'number of bars', 'number of beats'
    ,'number of sections', 'number of tatums' 
])

# Add a date of datapull
today_timestamp = pd.to_datetime("today")
today_date = today_timestamp.date()
df['date of data pull'] = today_date
df['date of data pull'] = pd.to_datetime(df['date of data pull'])


mlb = MultiLabelBinarizer()

artist_genres_dummies = pd.DataFrame(mlb.fit_transform(df['artist genres']),columns=mlb.classes_, index=df.index)

final_df = pd.concat([df,artist_genres_dummies], axis = 1)

upload_data_to_mysql(df):