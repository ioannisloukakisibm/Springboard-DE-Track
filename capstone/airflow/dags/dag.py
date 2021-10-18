import pandas as pd
import numpy as np

# Ignore harmless warnings
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime, timedelta, date

from scripts.data_pull_airflow import *
from scripts.preprocessing_airflow import *
from scripts.modeling_airflow import *

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'start_date':'2021-10-17'
    ,'schedule_interval':'0 10 * * 1'
    ,'retries': 2
    ,'retry_delay': timedelta(minutes=5)
}

dag = DAG(
'main_dag',
default_args=default_args,
description='DAG to run the spotify pipeline',
)


t0 = PythonOperator(
    task_id = 'retrieve_track_ids_task'
    ,python_callable = retrieve_track_ids
    ,op_kwargs={'lower_bound':2021}
    ,dag=dag
)


t1 = PythonOperator(
    task_id = 'pull_song_characteristics_task'
    ,python_callable = retrieve_track_features_and_create_df
    ,dag=dag
)


t2 = PythonOperator(
    task_id = 'upload_recent_pull_to_database_task'
    ,python_callable = upload_data_to_mysql
    ,dag=dag
)


t3 = PythonOperator(
    task_id = 'download_entire_df_from_database_task'
    ,python_callable = retrieve_data_from_mysql
    ,dag=dag
)


t4 = PythonOperator(
    task_id = 'create_dummy_variables_task'
    ,python_callable = dummy_variables
    ,op_kwargs={'variable_for_dummies':'artist genres'}
    ,dag=dag
)


t5 = PythonOperator(
    task_id = 'remove_missing_values_task'
    ,python_callable = missingness
    ,dag=dag
)


t6 = PythonOperator(
    task_id = 'calculate_stds_task'
    ,python_callable = calculate_stds
    ,dag=dag
)


t7 = PythonOperator(
    task_id = 'remove_outliers_task'
    ,python_callable = outliers
    ,dag=dag
)


t8 = PythonOperator(
    task_id = 'cleanup_dirty_data_task'
    ,python_callable = cleanup_dirty_data
    ,dag=dag
)


t9 = PythonOperator(
    task_id = 'remove_duplication_task'
    ,python_callable = remove_duplication
    ,dag=dag
)


t10 = PythonOperator(
    task_id = 'prepare_for_modeling_task'
    ,python_callable = pre_modelling
    ,dag=dag
)



t11 = PythonOperator(
    task_id = 'split_train_validation_test_task'
    ,python_callable = split_train_validation_test
    ,dag=dag
)


t12 = PythonOperator(
    task_id = 'select_appropriate_features_rf_task'
    ,python_callable = select_appropriate_features_rf
    ,op_kwargs={'target':'song popularity'}
    ,dag=dag
)


t13 = PythonOperator(
    task_id = 'select_appropriate_features_xgb_task'
    ,python_callable = select_appropriate_features_xgb
    ,op_kwargs={'target':'song popularity'}
    ,dag=dag
)


t14 = PythonOperator(
    task_id = 'train_model_rf_task'
    ,python_callable = train_model_rf
    ,op_kwargs={'target':'song popularity'}
    ,provide_context = True
    ,dag=dag
)


t15 = PythonOperator(
    task_id = 'train_model_xgb_task'
    ,python_callable = train_model_xgb
    ,op_kwargs={'target':'song popularity'}
    ,provide_context = True
    ,dag=dag
)


t16 = PythonOperator(
    task_id = 'generate_validation_predictions_rf_xgb_task'
    ,python_callable = generate_predictions_rf_xgb
    ,op_kwargs={'target':'song popularity', 'input_df_to_read':'validation.csv'}
    ,provide_context = True
    ,dag=dag
)


t17 = PythonOperator(
    task_id = 'calculate_weights_task'
    ,python_callable = calculate_weights
    ,provide_context = True
    ,dag=dag
)


t18 = PythonOperator(
    task_id = 'final_ensemble_prediction_task'
    ,python_callable = final_ensemble_prediction
    ,op_kwargs={'target':'song popularity', 'input_df_to_read':'test.csv'}
    ,provide_context = True
    ,dag=dag
)


t0>>t1>>t2>>t3>>t4>>t5>>t6>>t7>>t8>>t9>>t10>>t11>>t12>>t13>>t14>>t15>>t16>>t17>>t18

