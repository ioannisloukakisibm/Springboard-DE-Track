import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time 
import requests
import json
import openpyxl
import logging

import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import *

from datetime import datetime
from decouple import config

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

client_id = config('spotify_client_id')
client_secret = config('spotify_client_secret')

client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def retrieve_track_ids(year):
    list_of_track_ids = []
    for i in range(0,100000,50):
        try:
            track_results = sp.search(q=f'year:{year}', type='track', limit=50,offset=i, market='US')
            for i in range(len(track_results.get('tracks').get('items'))):
                list_of_track_ids.append(track_results.get('tracks').get('items')[i].get('id'))
            time.sleep(.5)
        except:
            break
    return list_of_track_ids


def getTrackFeatures(id):
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    meta = sp.track(id)
    features = sp.audio_features(id)
    audio_analysis = sp.audio_analysis(id)
    
    artist_url = meta['artists'][0]["external_urls"]["spotify"]
    artist = sp.artist(artist_url)

    album_url = meta['album']["external_urls"]["spotify"]
    album = sp.album(album_url)
    
    # album and artist info
    artist_genres = artist["genres"]
    artist_popularity = artist["popularity"]
    artist_no_of_followers = artist["followers"].get('total')
    artist_type = artist["type"]

    album_label = album['label']
    album_popularity = album['popularity']
  
    # meta
    song_id = id 
    song_name = meta['name']
    album = meta['album']['name']
    artist = meta['album']['artists'][0]['name']
    song_release_date = meta['album']['release_date']
    length = meta['duration_ms']
    song_popularity = meta['popularity']

    # features
    key = features[0]['key']
    mode = features[0]['mode']
    acousticness = features[0]['acousticness']
    valence = features[0]['valence']
    danceability = features[0]['danceability']
    energy = features[0]['energy']
    instrumentalness = features[0]['instrumentalness']
    liveness = features[0]['liveness']
    loudness = features[0]['loudness']
    speechiness = features[0]['speechiness']
    tempo = features[0]['tempo']
    time_signature = features[0]['time_signature']
    
    tempo_confidence = audio_analysis.get('track').get('tempo_confidence')
    key_confidence = audio_analysis.get('track').get('key_confidence')
    time_signature_confidence = audio_analysis.get('track').get('time_signature_confidence')
    mode_confidence = audio_analysis.get('track').get('mode_confidence')
    rhythm_version = audio_analysis.get('track').get('rhythm_version')
    synch_version = audio_analysis.get('track').get('synch_version')
    number_of_segments = len(audio_analysis.get('segments'))
    number_of_bars = len(audio_analysis.get('bars'))
    number_of_beats = len(audio_analysis.get('beats'))
    number_of_sections = len(audio_analysis.get('sections'))
    number_of_tatums = len(audio_analysis.get('tatums'))

    
    track = [song_id, song_name, album, artist, artist_genres, artist_popularity, artist_no_of_followers, artist_type
             ,album_label, album_popularity, song_release_date, length, song_popularity, key, mode
             ,acousticness,valence, danceability, energy, instrumentalness, liveness, loudness, speechiness, tempo
             ,time_signature, tempo_confidence, key_confidence, time_signature_confidence, mode_confidence
             ,rhythm_version, synch_version, number_of_segments, number_of_bars, number_of_beats, number_of_sections
            ,number_of_tatums 
]
    return track


ISSUE:
# VERBOSE SPOTIFY FUNCTION
# See if the below works:
# To avoid all the INFO logs from Spark appearing in the Console, set the log level as ERROR:

# 1 spark.sparkContext.setLogLevel("ERROR")

def upload_data_to_mysql(df):
    password = config('mysql_password')
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@localhost:3306/spotify')
    connection = engine.connect()

    try:
        df.to_sql(name = 'main', con = engine, if_exists = 'fail', chunksize = 1000, index=False)
        logging.debug('MySQL database was empty. Uploaded the latest dataset')

    except:
        df.to_sql(name = 'main', con = engine, if_exists = 'append', chunksize = 1000, index=False)
        logging.debug(f'Uploaded {df.shape[0]} rows to the database')
