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

from sklearn.preprocessing import MultiLabelBinarizer

import mysql
import mysql.connector

from pyspark.sql import SparkSession

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

client_id = config('spotify_client_id')
client_secret = config('spotify_client_secret')

client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def retrieve_track_ids_iterator_function(year):
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


def retrieve_track_ids(lower_bound, upper_bound = datetime.now().year + 1):

    year_list = list(np.arange(lower_bound,upper_bound))

    track_ids = []

    for year in year_list:
        holder_list = []
        holder_list = retrieve_track_ids_iterator_function(year)
        track_ids.extend(holder_list)

    logging.debug(f'pulled {len(track_ids)} track ids')

    df_of_track_ids = pd.DataFrame({'track_id':track_ids})

    df_of_track_ids.to_csv('track_ids.csv')

    return None


def retrieve_track_features_iterator_function(id):
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


def retrieve_track_features_and_create_df():

    df_of_track_ids = pd.read_csv('track_ids.csv')
    
    list_of_track_ids = list(df_of_track_ids['track_id'])

    list_of_track_features = []
    
    for i in range(len(list_of_track_ids)):
    #     time.sleep(.5)
        try:
            track_features = retrieve_track_features_iterator_function(list_of_track_ids[i])
            list_of_track_features.append(track_features)
        except:
            continue

    df = pd.DataFrame(list_of_track_features, columns = [
        'song_id', 'song name', 'album', 'artist', 'artist genres', 'artist popularity', 'artist number of followers'
        ,'artist type', 'album label', 'album popularity', 'song release date', 'length', 'song popularity'
        ,'key', 'mode', 'acousticness','valence', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness'
        ,'speechiness','tempo', 'time signature', 'tempo confidence', 'key confidence', 'time signature confidence'
        ,'mode confidence','rhythm version', 'synch version', 'number of segments', 'number of bars', 'number of beats'
        ,'number of sections', 'number of tatums' 
    ])

    # Add a date of data pull
    today_timestamp = pd.to_datetime("today")
    today_date = today_timestamp.date()
    df['date of data pull'] = today_date
    df['date of data pull'] = pd.to_datetime(df['date of data pull'])

    df['artist genres'] = df['artist genres'].astype('str')

    logging.debug(f'pulled the features of {df.shape[0]} tracks')

    df.to_csv('final_raw_data_pull_from_api.csv')

    return None


def upload_data_to_mysql():

    df = pd.read_csv('final_raw_data_pull_from_api.csv')

    password = config('mysql_password')
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@localhost:3306/spotify')
    connection = engine.connect()
    logging.debug('Connection to MySQL was successful')

    try:
        df.to_sql(name = 'song_attributes_raw', con = engine, if_exists = 'fail', chunksize = 1000, index=False)
        logging.debug('MySQL database was empty. Uploaded the latest dataset')

    except:
        df.to_sql(name = 'song_attributes_raw', con = engine, if_exists = 'append', chunksize = 1000, index=False)
        logging.debug(f'Uploaded {df.shape[0]} rows to the database')


def get_db_connection():
    connection = None
    try:
        password = config('mysql_password')

        connection = mysql.connector.connect(
            user='root'
            ,password=f'{password}' 
            ,host='localhost' 
            ,port='3306' 
            ,database='spotify')
        logging.debug('Connection to MySQL was established')

    except:
        logging.critical('Failed to connect to MySQL Database')

    return connection


def delete_data_from_mysql():
    sql_statement = "drop table if exists spotify.song_attributes_raw"
    connection = get_db_connection()
    cursor=connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()

    logging.debug(f'Table deleted from mysql database')

    return None


def retrieve_data_from_mysql():
    sql_statement = "select * from spotify.song_attributes_raw"
    connection = get_db_connection()
    cursor=connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()

    database_dataset = pd.DataFrame(records, columns = list(cursor.column_names))

    logging.debug(f'We pulled {database_dataset.shape[0]} rows from mysql database')

    database_dataset.to_csv('entire df from database.csv')

    return None


def spark_db_connection():
    
    spark = SparkSession.builder.config("spark.jars", "C:\Program Files (x86)\MySQL\Connector J 8.0\mysql-connector-java-8.0.26.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()

    df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spotify") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "song_attributes_raw") \
    .option("user", "root").option("password", config('mysql_password')).load()

    spark.stop()

    return df
