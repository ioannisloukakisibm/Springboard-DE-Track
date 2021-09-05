import numpy as np
import pandas as pd

import ast

from sklearn.preprocessing import MultiLabelBinarizer

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def dummy_variables(input_df, variable_for_dummies):

    input_df[variable_for_dummies] = input_df[variable_for_dummies].apply(lambda s: list(ast.literal_eval(s)))
    
    mlb = MultiLabelBinarizer()
    artist_genres_dummies = pd.DataFrame(mlb.fit_transform(input_df[variable_for_dummies])
                                        ,columns=mlb.classes_
                                        ,index=input_df.index)

    merged = pd.concat([input_df,artist_genres_dummies], axis = 1)

    list_of_dummy_columns = list(artist_genres_dummies.columns)
    list_of_non_dummy_columns = [col for col in merged.columns if col not in list_of_dummy_columns]


    logging.debug(f'Dummy variables were successfully created')

    return merged, list_of_dummy_columns, list_of_non_dummy_columns


def missingness(input_df):
    
    input_df.fillna({'album label':'no info', 'artist number of followers':0}, inplace = True)
    input_df.dropna(inplace = True)

    logging.debug(f'The data set has now 0 missing values in all fields')

    return input_df


def calculate_stds(input_df):

    std_vars = ['artist popularity', 'artist number of followers', 'album popularity', 'song popularity']
    std_vars_2 = []

    for var_ in std_vars:
        input_df[f'{var_} std'] = \
        input_df.groupby('song_id', as_index = False)[var_].transform(lambda s: s.std())
        
        std_vars_2.append(f'{var_} std')

    logging.debug(f'STDs were successfully calculated')

    return std_vars_2, input_df


def outliers(input_df):

    df1 = input_df.copy()
    original_df_size = df1.shape[0]

    df1 = df1[
        (df1['artist number of followers'] > 0)
        &
        (df1['tempo confidence'] > 0)
        &
        (df1['key confidence'] > 0)
        &
        (df1['time signature confidence'] > 0)
        &
        (df1['mode confidence'] > 0)
    ]

    df1.drop(columns = ['rhythm version', 'synch version'], inplace = True)

    processed_df_size = df1.shape[0]

    logging.debug(f'Outliers were successfully removed: The Data Frame lost {original_df_size - processed_df_size} obs to a total of {processed_df_size}')

    return df1


def cleanup_dirty_data(input_df, std_vars):

    df1 = input_df.copy()
    original_df_size = df1.shape[0]

    d={}

    for std_var in std_vars:
 
        pctl = input_df[std_var].describe(percentiles=np.linspace(0,1,101))
        d[f'{std_var} 90th'] = pctl[94]
        d[f'{std_var} 95th'] = pctl[99]
        d[f'{std_var} 99th'] = pctl[103]


    df1 = df1[
        ~(
        (df1['artist popularity std']>d.get('artist popularity std 95th'))
        |
        ((df1['song popularity std']>d.get('song popularity std 90th')) & (df1['song popularity'] == 0))
        |
        (df1['song popularity std']>d.get('song popularity std 99th'))
        |
        ((df1['album popularity std']>d.get('album popularity std 90th')) & (df1['album popularity'] == 0))
        |
        (df1['album popularity std']>d.get('album popularity std 99th'))
        )
    ]

    processed_df_size = df1.shape[0]

    logging.debug(f'dirty obs were successfully removed: The Data Frame lost {original_df_size - processed_df_size} obs to a total of {processed_df_size}')

    return df1


def remove_duplication(input_df):

    df1 = input_df.copy()

    original_df_size = df1.shape[0]

    confidence_vars = ['tempo confidence', 'key confidence', 'time signature confidence', 'mode confidence']

    for var_ in confidence_vars:
        df1[f'{var_} max_confidence'] = \
        df1.groupby('song_id', as_index = False)[var_].transform(lambda s: s.max())

        df1 = df1[df1[var_] == df1[f'{var_} max_confidence']]

    processed_df_size = df1.shape[0]

    logging.debug(f'Duplicates were successfully removed: The Data Frame lost {original_df_size - processed_df_size} obs to a total of {processed_df_size}')

    return df1


def pre_modelling(df, list_of_dummy_columns, list_of_non_dummy_columns):

    list_of_dummy_columns.append('song_id')
    list_of_dummy_columns.append('song release date')
    list_of_non_dummy_columns.remove('rhythm version')
    list_of_non_dummy_columns.remove('synch version')
 
    df_baseline = df[list_of_non_dummy_columns].groupby(['song_id', 'song release date'], as_index = False).mean()
    df_dummies =  df[list_of_dummy_columns].groupby(['song_id', 'song release date'], as_index = False).max()
    merged = pd.merge(df_baseline, df_dummies, how = 'inner', on = ['song_id', 'song release date'])

    initial_n = df.shape[0]
    df_baseline_n = df_baseline.shape[0]
    df_dummies_n = df_dummies.shape[0]
    merged_n = merged.shape[0]

    if (df_baseline_n == df_dummies_n) & (df_baseline_n == merged_n):
        logging.debug(f'The final merge was successfully completed: No data was lost')
        logging.debug(f'The final datarame contains: {merged_n} unique song_ids')
    else:
        logging.debug(f'The final merge was successfully completed but we lost {df_baseline_n-merged_n} obs')

    return merged
