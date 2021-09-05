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

from data_pull import *
from preprocessing import *
from modeling import *

import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import *

from datetime import datetime
from decouple import config

from mpire import WorkerPool

from sklearn.preprocessing import MultiLabelBinarizer

import time

start_time = time.time()

list_of_track_ids = retrieve_track_ids(2018)
list_of_track_features = retrieve_track_features(list_of_track_ids)
final_df = create_final_dataset(list_of_track_features)
upload_data_to_mysql(final_df)
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

final_df = pre_modelling(df_with_dummies_and_stds_and_no_outliers_clean_deduped
                        ,list_of_dummy_columns
                        ,list_of_non_dummy_columns
                        )

# final_df.to_csv('test_modeling DELETE.csv')

# final_df = pd.read_csv('test_modeling DELETE.csv')

input_features = list(final_df.columns)
input_features.remove('song_id')
input_features.remove('song release date')

train, validation, test = split_train_validation_test(final_df)
selected_rf_features = select_appropriate_features_rf(train, 'song popularity', input_features)
selected_xgb_features = select_appropriate_features_xgb(train, 'song popularity', input_features)
fitted_rf, rf_importance = train_model_rf(train,'song popularity',selected_rf_features)
fitted_xgb, xgb_importance = train_model_xgb(train,'song popularity',selected_xgb_features)

rf_xgb_predictions = generate_predictions_rf_xgb(validation
                                                ,'song popularity'
                                                ,selected_rf_features
                                                ,selected_xgb_features
                                                ,fitted_rf, fitted_xgb)

rf_xgb_predictions.head()

weight_rf, weight_xgb = calculate_weights(rf_xgb_predictions)

final_prediction = final_ensemble_prediction(test
                                            ,'song popularity'
                                            ,selected_rf_features
                                            ,selected_xgb_features
                                            ,fitted_rf
                                            ,fitted_xgb
                                            ,weight_rf
                                            ,weight_xgb)

final_prediction.to_csv('check final ensemble output DELETE.csv')

end_time = time.time()

print(f"Elapsed time: {(end_time - start_time)/ 60:.2f} minutes")
