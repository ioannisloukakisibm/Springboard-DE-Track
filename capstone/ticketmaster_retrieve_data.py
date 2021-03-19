import logging
import sys

import requests
import json

import pandas as pd
import numpy as np
import openpyxl

from datetime import datetime
from decouple import config

import mysql
import mysql.connector
from sqlalchemy import *

from functions_for_data_retrieval import *

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

api_key = config('ticketmaster_apikey')
password = config('mysql_password')


# Retrieve the data from the Ticketaster API
list_of_triangle_events = retrieve_data_from_api()

# Put all the events in a dataframe
events = pd.DataFrame({'events':list_of_triangle_events})

# Break the disctionaries into columns
# Now we have 1 column for each sales, locale, dates, etc
event_breakout = events['events'].apply(pd.Series)


# Extract the data from the event_breakout
event_names_df = get_main_event_info(list_of_triangle_events)
venue_final = get_venue_info(event_breakout)
public_sales_final = get_public_sale_info(event_breakout)
presales_final =  get_presales_info(event_breakout)
start_dates_final, status_final, initial_start_date_final = get_date_info(event_breakout)
presales_final =  get_presales_info(event_breakout)
classification_df_final = get_classification_info(event_breakout)
promoters_df_final = get_promoter_info(event_breakout)
pricerange_df_final = get_pricing_info(event_breakout)
ticketlimit_df_final = get_ticket_limit_info(event_breakout)
please_note_df = pd.DataFrame(event_breakout['pleaseNote'])



# MERGE all the data extraction datasets into a final dataset
api_dataset = pd.concat([
    event_names_df
    ,venue_final 
    ,event_breakout['info']
    ,public_sales_final
    ,presales_final
    ,start_dates_final
    ,status_final
    ,initial_start_date_final
    ,classification_df_final
    ,promoters_df_final
    ,pricerange_df_final
    ,ticketlimit_df_final
    ,please_note_df
    
], axis = 1)


today_timestamp = pd.to_datetime("today")
api_dataset['timestamp of data pull'] = today_timestamp
api_dataset['date of data pull'] = api_dataset['timestamp of data pull'].dt.date

api_dataset = rearrange_columns(api_dataset)


try:
    database_dataset = download_data_from_mysql()

except:
    upload_data_to_mysql_if_empty(api_dataset)
    sys.exit(0)
    pass


appended_dataset =  append_api_data_to_database_data(database_dataset, api_dataset) 

updated_dataset = clean_and_dedup_the_appended_set(appended_dataset) 

upload_the_dataset_if_extra_rows(updated_dataset, database_dataset)