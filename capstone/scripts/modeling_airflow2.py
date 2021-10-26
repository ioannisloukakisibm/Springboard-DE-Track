import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def split_train_validation_test():

    input_df = pd.read_csv('/usr/local/tmp_data/df_with_genre_dummies_no_missing_std_no_outliers_cleaned_dedupped_ready_for_modeling.csv')

    input_df['song release date'] = pd.to_datetime(input_df['song release date'])
    input_df['song popularity'] = input_df['song popularity'] + 1

    for i in range(12):

        test = input_df[input_df['song release date'] > (datetime.now() - relativedelta(months=i))]

        if (test.shape[0] > 0) |  (datetime.now().month - i <=0):
            break

    test['merge_dummy'] = 1

    remainder_df = pd.merge(input_df, test[['song_id', 'song release date', 'merge_dummy']]
                            ,how = 'outer'
                            ,on = ['song_id', 'song release date'])

    initial = remainder_df.shape[0]
    remainder_df = remainder_df[remainder_df['merge_dummy']!=1]
    remainder = remainder_df.shape[0]

    test.drop(columns = ['merge_dummy'], inplace = True)
    remainder_df.drop(columns = ['merge_dummy'], inplace = True)

    msk = np.random.rand(len(remainder_df)) < 0.8
    train = remainder_df[msk]
    validation = remainder_df[~msk]

    logging.debug(f'The test dataframe has {test.shape[0]} obs, derived from a {i} month period')
    logging.debug(f'if {initial - remainder} is equal to {test.shape[0]} then no data were lost')
    logging.debug(f'The remaining data were successfully split into 80/20 train - validation')

    train.to_csv('/usr/local/tmp_data/train.csv')
    validation.to_csv('/usr/local/tmp_data/validation.csv')
    test.to_csv('/usr/local/tmp_data/test.csv')

    return None


def select_appropriate_features_rf(target):

    input_df = pd.read_csv('/usr/local/tmp_data/train.csv')
    input_features = list(input_df.columns)
    input_features.remove('song_id')
    input_features.remove('song release date')
    input_features.remove(target)

    x = input_df[input_features]
    y = input_df[target].values.reshape(-1, 1)

    number_of_selected_features = int(round(input_df.shape[0]/150,0))

    rf = RandomForestRegressor(random_state=25,  max_features=100, n_estimators=1000)
    fitted_rf = rf.fit(x, y)

    importance = fitted_rf.feature_importances_
    rf_importance = pd.DataFrame({'feature': list(x.columns), 'importance': list(importance)})\
        .sort_values(by=['importance'], ascending=False)

    selected_rf_features = list(rf_importance.iloc[:number_of_selected_features]['feature'])

    selected_rf_features_df = pd.DataFrame({'var_name':selected_rf_features})
    selected_rf_features_df.to_csv('/usr/local/tmp_data/selected_rf_features.csv')

    logging.debug(f'we selected {len(selected_rf_features)} features for the Random Forest out of an initial pool of {len(input_features)}')

    return None


def select_appropriate_features_xgb(target):

    input_df = pd.read_csv('/usr/local/tmp_data/train.csv')
    input_features = list(input_df.columns)
    input_features.remove('song_id')
    input_features.remove('song release date')
    input_features.remove(target)

    x = input_df[input_features]
    y = input_df[target].values.reshape(-1, 1)

    number_of_selected_features = int(round(input_df.shape[0]/150,0))

    xgb_model = xgb.XGBRegressor(n_estimators=750, n_jobs=6, learning_rate=0.1, seed=25)
    fitted_xgb = xgb_model.fit(x, y, eval_metric="mae", verbose=0)
    importance = fitted_xgb.feature_importances_
    xgb_importance = pd.DataFrame({'feature': list(x.columns), 'importance': list(importance)})\
        .sort_values(by=['importance'], ascending=False)

    selected_xgb_features = list(xgb_importance.iloc[:number_of_selected_features]['feature'])

    selected_xgb_features_df = pd.DataFrame({'var_name':selected_xgb_features})
    selected_xgb_features_df.to_csv('/usr/local/tmp_data/selected_xgb_features.csv')

    logging.debug(f'we selected {len(selected_xgb_features)} features for the XGBoost out of an initial pool of {len(input_features)}')

    return None


def train_model_rf(target):

    input_df = pd.read_csv('/usr/local/tmp_data/train.csv')
    selected_rf_features_df = pd.read_csv('/usr/local/tmp_data/selected_rf_features.csv')
    selected_rf_features = list(selected_rf_features_df['var_name'])

    x = input_df[selected_rf_features]
    y = input_df[target].values.reshape(-1, 1)

    rf = RandomForestRegressor(random_state=25,  max_features=50, n_estimators=500)
    fitted_rf = rf.fit(x, y)

    importance = fitted_rf.feature_importances_
    rf_importance = pd.DataFrame({'feature': list(x.columns), 'importance': list(importance)})\
        .sort_values(by=['importance'], ascending=False)

    rf_importance.to_csv('/usr/local/tmp_data/rf_feature_importance.csv')  

    logging.debug(f'Random Forest model was trained successfully')

    return fitted_rf


def train_model_xgb(target):

    input_df = pd.read_csv('/usr/local/tmp_data/train.csv')
    selected_xgb_features_df = pd.read_csv('/usr/local/tmp_data/selected_xgb_features.csv')
    selected_xgb_features = list(selected_xgb_features_df['var_name'])

    x = input_df[selected_xgb_features]
    y = input_df[target].values.reshape(-1, 1)

    xgb_model = xgb.XGBRegressor(n_estimators=750, n_jobs=6, learning_rate=0.1, seed=25)
    fitted_xgb = xgb_model.fit(x, y, eval_metric="mae", verbose=0)
    importance = fitted_xgb.feature_importances_
    xgb_importance = pd.DataFrame({'feature': list(x.columns), 'importance': list(importance)})\
        .sort_values(by=['importance'], ascending=False)

    xgb_importance.to_csv('/usr/local/tmp_data/xgb_feature_importance.csv')  

    logging.debug(f'XGBoost model was trained successfully')

    return fitted_xgb


def calculate_weights(input_df):

    errors_rf = np.abs(input_df['Random Forest'] - input_df['actuals'])
    mape_rf = (errors_rf / input_df['actuals'])
    accuracy_rf = 1 - np.mean(mape_rf)
    logging.debug(f'Random Forest Accuracy: {round(accuracy_rf, 2)}')

    errors_xgb = np.abs(input_df['XGBoost'] - input_df['actuals'])
    mape_xgb = (errors_xgb / input_df['actuals'])
    accuracy_xgb = 1 - np.mean(mape_xgb)
    logging.debug(f'XGBoost Accuracy: {round(accuracy_xgb, 2)}')

    weight_rf = accuracy_rf/(accuracy_rf + accuracy_xgb) 
    weight_xgb = accuracy_xgb/(accuracy_rf + accuracy_xgb) 

    return weight_rf, weight_xgb



def generate_predictions_rf_xgb(target):

    validation_input_df = pd.read_csv('/usr/local/tmp_data/validation.csv')
    test_input_df = pd.read_csv('/usr/local/tmp_data/test.csv')
    selected_rf_features_df = pd.read_csv('/usr/local/tmp_data/selected_rf_features.csv')
    selected_rf_features = list(selected_rf_features_df['var_name'])
    selected_xgb_features_df = pd.read_csv('/usr/local/tmp_data/selected_xgb_features.csv')
    selected_xgb_features = list(selected_xgb_features_df['var_name'])

    fitted_rf = train_model_rf(target) 
    fitted_xgb = train_model_xgb(target) 

    validation_x_rf = validation_input_df[selected_rf_features]
    validation_y_rf = fitted_rf.predict(validation_x_rf)

    validation_x_xgb = validation_input_df[selected_xgb_features]
    validation_y_xgb = fitted_xgb.predict(validation_x_xgb)

    validation_outcomes = pd.DataFrame({
        'actuals': list(validation_input_df[target])
        , 'Random Forest': list(validation_y_rf)
        , 'XGBoost': list(validation_y_xgb)
    })

    logging.debug(f'Validation successfully completed')

    weight_rf, weight_xgb = calculate_weights(validation_outcomes)

    test_x_rf = test_input_df[selected_rf_features]
    test_y_rf = fitted_rf.predict(test_x_rf)

    test_x_xgb = test_input_df[selected_xgb_features]
    test_y_xgb = fitted_xgb.predict(test_x_xgb)

    test_outcomes = pd.DataFrame({
        'song_id': list(test_input_df['song_id'])
        # ,'song name': list(test_input_df['song name'])
        # ,'Artist': list(test_input_df['artist'])
        ,'actuals': list(test_input_df[target])
        , 'Random Forest': list(test_y_rf)
        , 'XGBoost': list(test_y_xgb)
    })

    test_outcomes['ensemble_prediction'] = test_outcomes['Random Forest']*weight_rf + test_outcomes['XGBoost']*weight_xgb
    test_outcomes['delta prediction'] = test_outcomes['ensemble_prediction'] - test_outcomes['actuals']

    test_outcomes.sort_values(by = ['delta prediction'], ascending = False, inplace = True)

    logging.debug(f'Predictions were successfully generated')
    logging.debug(f'The ensemble was weighted as {weight_rf} on the Random Forest and {weight_xgb} on the XGBoost')

    test_outcomes.to_csv('/usr/local/tmp_data/final_predictions.csv', index = False)

    return None

# def final_formating():

#     predictions = pd.read_csv('/usr/local/tmp_data/final_predictions.csv', index = False)
#     baseline = pd.read_csv('entire df from database.csv', usecols = ['song_id','song name', 'artist'])




# def final_ensemble_prediction(input_df, target, **context):

#     weight_rf = context['ti'].xcom_pull(key='weight_rf') 
#     weight_xgb = context['ti'].xcom_pull(key='weight_xgb') 

#     outcomes = generate_predictions_rf_xgb(target, input_df)

#     outcomes['ensemble_prediction'] = outcomes['Random Forest']*weight_rf + outcomes['XGBoost']*weight_xgb
#     outcomes['delta prediction'] = outcomes['ensemble_prediction'] - outcomes['actuals']

#     outcomes.sort_values(by = ['delta prediction'], ascending = False, inplace = True)

#     logging.debug(f'Predictions were successfully generated')
#     logging.debug(f'The ensemble was weighted as {weight_rf} on the Random Forest and {weight_xgb} on the XGBoost')

#     outcomes.to_csv('/usr/local/tmp_data/final_predictions.csv')

#     return None     
