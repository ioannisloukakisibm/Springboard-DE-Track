import numpy as np
from scipy.sparse.sputils import check_shape
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time 
import requests
import json
import openpyxl
import logging
import ast

from functions_for_spotify_data_retrieval import *
from preprocessing import *

import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import *

from datetime import datetime
from decouple import config

from mpire import WorkerPool

from sklearn.preprocessing import MultiLabelBinarizer

import time
from time import process_time

# list_of_track_ids = retrieve_track_ids(2016)
# list_of_track_features = retrieve_track_features(list_of_track_ids)
# final_df = create_final_dataset(list_of_track_features)
# upload_data_to_mysql(final_df)
df = retrieve_data_from_mysql()

df_with_dummies, list_of_dummy_columns, list_of_non_dummy_columns = dummy_variables(df, 'artist genres')

df_with_dummies = missingness(df_with_dummies)
std_vars, df_with_dummies_and_stds = calculate_stds(df_with_dummies)
df_with_dummies_and_stds_and_no_outliers = outliers(df_with_dummies_and_stds)

df_with_dummies_and_stds_and_no_outliers_clean = cleanup_dirty_data(
    df_with_dummies_and_stds_and_no_outliers
    ,std_vars)

df_with_dummies_and_stds_and_no_outliers_clean_deduped = remove_duplication(
    df_with_dummies_and_stds_and_no_outliers_clean)

# df_with_dummies_and_stds_and_no_outliers_clean_deduped.to_csv('check if the pipeline run E2E DELETE.csv')

final_df = pre_modelling(df_with_dummies_and_stds_and_no_outliers_clean_deduped
                        ,list_of_dummy_columns
                        ,list_of_non_dummy_columns
                        )

