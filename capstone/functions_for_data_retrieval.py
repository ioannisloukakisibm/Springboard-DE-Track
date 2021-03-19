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


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def retrieve_data_from_api():

    api_key = config('ticketmaster_apikey')

    # pick up Carolina Theater separately, because for some reason it is not part of the Triangle locale. 
    url = f'https://app.ticketmaster.com/discovery/v2/events.json?size=200&venueId=KovZpZAFAl6A&apikey={api_key}'

    response = requests.get(url)
    data=response.text

    parsed = json.loads(data)

    list_of_carolina_theater_events = parsed.get('_embedded').get('events')
    list_of_triangle_events = []
    index = 1

    while True:

        url = f'https://app.ticketmaster.com/discovery/v2/events.json?page={index}&dmaId=366&size=200&apikey={api_key}'

        response = requests.get(url)
        data=response.text

        parsed = json.loads(data)

        # Combine all the events
        try:
            list_of_triangle_events.extend(parsed.get('_embedded').get('events'))
            index+=1
        except:
            logging.debug(f"successfully pulled {index-1} Triangle pages and {len(list_of_triangle_events)} events")
            break

    list_of_triangle_events.extend(list_of_carolina_theater_events)

    logging.debug(f'Added {len(list_of_carolina_theater_events)} Carolina Theater events for a total of {len(list_of_triangle_events)} Triangle events')

    return list_of_triangle_events


def get_main_event_info(list_of_events):
    list_of_event_names = [event.get('name') for event in list_of_events]
    list_of_event_ids = [event.get('id') for event in list_of_events]
    test = [event.get('test') for event in list_of_events]

    event_names_df = pd.DataFrame({'event name':list_of_event_names, 'event id':list_of_event_ids, 'is this entry a test?':test})

    logging.debug('Extracted the main info of events')

    return event_names_df


def get_venue_info(event_breakout):
    embedded_df = event_breakout._embedded.apply(pd.Series)

    list_of_venue_dictionaries = [embedded_df.venues[index][0] for index in range(len(embedded_df.venues))] 

    venue_df = pd.DataFrame({'venue_dictionaries':list_of_venue_dictionaries})
    venue_final = pd.DataFrame(venue_df['venue_dictionaries'].apply(pd.Series)['name'])
    venue_final.rename({'name': 'venue name'}, axis = 1, inplace = True)
    logging.debug('Extracted the venue info')

    return venue_final


def get_public_sale_info(event_breakout):
    # Break the sales column into columns
    # We now have public sales and presales
    sales = event_breakout.sales.apply(pd.Series)

    # Break the public sales into columns
    public_sales = sales.public.apply(pd.Series)

    # rename the public sales columns to distinguish them from the rest
    for col in public_sales.columns:
        public_sales.rename({col:f'public sales {col}'}, axis = 1, inplace = True)
    
    logging.debug('Extracted the public sale info')
    return public_sales

        

def get_presales_info(event_breakout):
    # Break the sales column into columns
    # We now have public sales and presales
    sales = event_breakout.sales.apply(pd.Series)

    # Create a dataframe to add the final product of the steps below
    presales_final = pd.DataFrame()

    # Looping through the presales column to extract the information from the dictionaries
    # each presales row will have a dictionary with all the presale methods 
    for presales_list_element in sales.presales:
        
    # """
    # If there is no presale, the entire rowcell will be NaN
    # In this case the pandas.isnull method will give us an error
    # We will need to add a new blank row in the except statement
    # We will also add a new column called 'blank' to make sure we don't mess the other columns names
    # The column will be dropped at the end
    # """
        try:
            length = len(pd.isnull(presales_list_element))
        except:
            dict_holder_df = pd.DataFrame({'blank':[np.nan]})
            presales_final = pd.concat([presales_final,dict_holder_df],axis=0)
            continue

    # """
    # The dictionaries will contain the name of the presale as well as the end and start date. 
    # We need to extract the name and move it to the column names so that we can distinguish the start and end dates of each presale
    # """
        dict_holder_df = pd.DataFrame()
        for presales_dict in presales_list_element:
            
            start_time_title = presales_dict['name'] + ' ' + list(presales_dict.keys())[0]
            end_time_title = presales_dict['name'] + ' ' + list(presales_dict.keys())[1]
            
            holder_df = pd.DataFrame({start_time_title:[presales_dict['startDateTime']], end_time_title:[presales_dict['endDateTime']]})
            dict_holder_df = pd.concat([dict_holder_df,holder_df], axis=1)
            
        presales_final = pd.concat([presales_final,dict_holder_df],axis=0)


    # Reset the index so that we can merge
    # drop the blank column
    presales_final.reset_index(inplace=True)
    presales_final.drop(columns=['index', 'blank'], inplace=True)

    logging.debug('Extracted the presales info')

    return presales_final



def get_date_info(event_breakout):
    # Break the dates column into columns
    # There are a lot of dates trapped into disctionaries. Use pd.Series multiple times to untangle everything
    dates = event_breakout.dates.apply(pd.Series)
    start_dates = dates.start.apply(pd.Series)
    status = dates.status.apply(pd.Series)
    initial_start_date = dates.initialStartDate.apply(pd.Series)

    # Make sure there are no weird columns created by pd.series
    initial_start_date = initial_start_date[['dateTime', 'localDate', 'localTime']]

    # rename the dates columns to distinguish them
    for col in start_dates.columns:
        start_dates.rename({col:f'event start {col}'}, axis = 1, inplace = True)

    for col in initial_start_date.columns:
        initial_start_date.rename({col:f'event initial start {col}'}, axis = 1, inplace = True)

    logging.debug('Extracted the date info')

    return start_dates, status, initial_start_date



def get_classification_info(event_breakout):

    list_of_classification_dictionaries = [event_breakout.classifications[index][0] for index in range(len(event_breakout.classifications))] 

    classification_df = pd.DataFrame({'classification_dictionaries':list_of_classification_dictionaries})
    classification_df_broken_in_columns = classification_df['classification_dictionaries'].apply(pd.Series)


    classification_df_primary_family = classification_df_broken_in_columns[['primary', 'family']]
    columns_of_interest = list(classification_df_broken_in_columns.columns)
    columns_of_interest.remove('primary')
    columns_of_interest.remove('family')

    classification_df_final = pd.DataFrame()

    for column in columns_of_interest:
        series_holder = classification_df_broken_in_columns[column].apply(pd.Series)
        series_holder.drop(columns = ['id'], inplace=True)
        series_holder.rename({series_holder.columns[0]:column}, inplace = True, axis = 1)
        classification_df_final = pd.concat([classification_df_final,series_holder], axis = 1)

    logging.debug('Extracted the Classification info')

    return classification_df_final



def get_promoter_info(event_breakout):

    # Break the promoters column into columns
    promoters = event_breakout.promoters.apply(pd.Series)

    for index in range(len(promoters.columns)):
        promoters.rename({promoters.columns[index]: 'event promoter' + ' ' + str(promoters.columns[index]+1)}, axis = 1, inplace = True)

    promoters_df_final = pd.DataFrame()

    for column in promoters.columns:
        series_holder = promoters[column].apply(pd.Series)
        series_holder = series_holder[['name', 'description']]
        series_holder.rename({'name':column + ' ' + 'name', 'description':column + ' ' + 'description'}, inplace = True, axis = 1)
        promoters_df_final = pd.concat([promoters_df_final,series_holder], axis = 1)

    logging.debug('Extracted the Promoter info')

    return promoters_df_final



def get_pricing_info(event_breakout):

    # Sometimes the price information is missing from an event. 
    # Replace NA with the word missing
    event_breakout.fillna({'priceRanges':'missing'}, inplace=True)


    # create a genberic pricing structure to replace the missing pricing information
    # set all prices to zero
    generic_pricing_structure = [{'type': 'standard', 'currency': 'USD', 'min': -1, 'max': -1}]        

    # Unfortunately the replace will get rid of the list part and replace everything with a 'naked' dictionary
    # Ideally we would have a list with the generic pricing dictionary as its only element
    price_holder = event_breakout.replace({'missing':generic_pricing_structure})


    # The try-except step will fix this issue. 
    # If we have a list with a dictionary inside, we will fetch the dictionary from inside the list
    # Otherwise we will fetch the dictionary directly 
    list_of_pricerange_dictionaries = []

    for i, row in price_holder.iterrows():
        try:
            x = row['priceRanges'].get('type')
            list_of_pricerange_dictionaries.append(row['priceRanges'])
        except:
            list_of_pricerange_dictionaries.append(row['priceRanges'][0])

            
    pricerange_df = pd.DataFrame({'pricerange_dictionaries':list_of_pricerange_dictionaries})
    pricerange_df_broken_in_columns = pricerange_df['pricerange_dictionaries'].apply(pd.Series)


    pricerange_df_broken_in_columns = pricerange_df_broken_in_columns[['type', 'min', 'max']]
    pricerange_df_broken_in_columns.rename({'type':'price type', 'min':'price min', 'max':'price max'}, axis = 1, inplace = True)

    logging.debug('Extracted the Pricing info')

    return pricerange_df_broken_in_columns


def get_ticket_limit_info(event_breakout):
    ticket_limit = pd.DataFrame(event_breakout['ticketLimit'].apply(pd.Series))
    ticket_limit = pd.DataFrame(ticket_limit['info']) 
    ticket_limit.rename({'info':'ticket limit'},axis = 1, inplace = True)

    logging.debug('Extracted the Ticket Limit info')

    return ticket_limit


def rearrange_columns(df):
    holder_columns = list(df.columns)
    holder_columns.remove('timestamp of data pull')
    holder_columns.remove('date of data pull')
    all_columns = ['timestamp of data pull','date of data pull']
    all_columns.extend(holder_columns)
    df = df[all_columns]
    return df


def get_db_connection():
    connection = None
    try:
        password = config('mysql_password')

        connection = mysql.connector.connect(
            user='root'
            ,password=f'{password}' 
            ,host='localhost' 
            ,port='3306' 
            ,database='ticketmaster')
        logging.debug('Connection to MySQL was established')


    except:
        logging.critical('Failed to connect to MySQL Database')

    return connection



def download_data_from_mysql():
    sql_statement = "select * from ticketmaster.triangle"
    connection = get_db_connection()
    cursor=connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()

    database_dataset = pd.DataFrame(records, columns = list(cursor.column_names))
    database_dataset = rearrange_columns(database_dataset)

    logging.debug(f'We pulled {database_dataset.shape[0]} rows from mysql database')

    return database_dataset


def upload_data_to_mysql_if_empty(df):
    password = config('mysql_password')
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@localhost:3306/ticketmaster')
    connection = engine.connect()

    df = rearrange_columns(df)

    df.to_sql(name = 'triangle', con = engine, if_exists = 'fail', chunksize = 1000, index=False)
    logging.debug('MySQL database was empty. Uploaded the latest dataset')



def append_api_data_to_database_data(database_dataset, api_dataset): 

    appended_dataset = pd.concat([database_dataset, api_dataset], axis = 0).reset_index(drop=True)

    appended_dataset['timestamp of data pull'] = pd.to_datetime(appended_dataset['timestamp of data pull'])
    appended_dataset['date of data pull'] = appended_dataset['timestamp of data pull'].dt.date

    logging.debug(f'The appended api and database sets contain {appended_dataset.shape[0]} rows')

    return appended_dataset


def clean_and_dedup_the_appended_set(appended_dataset): 

    datatypes_df = pd.DataFrame(appended_dataset.dtypes, columns = ['data type']).reset_index()

    list_of_string_variables = list(datatypes_df[datatypes_df['data type'] == 'object']['index'])
    list_of_boolean_variables = list(datatypes_df[datatypes_df['data type'] == 'bool']['index'])
    list_of_numeric_variables = list(datatypes_df[datatypes_df['data type'] == 'float']['index'])

    list_of_groupby_columns = list(appended_dataset.columns)

    for var in list_of_groupby_columns:
        if var in list_of_string_variables:
            appended_dataset.fillna({var: '-1'}, inplace = True)
        else:
            appended_dataset.fillna({var: -1}, inplace = True)


    list_of_groupby_columns.remove('timestamp of data pull')
    list_of_groupby_columns.remove('date of data pull')

    
    updated_dataset = (
        appended_dataset
        .groupby(list_of_groupby_columns, as_index = False)
        .agg({'date of data pull':'min', 'timestamp of data pull':'min'})
    ) 

    updated_dataset = rearrange_columns(updated_dataset)

    logging.debug(f'After deduping we ended up with {updated_dataset.shape[0]} rows')

    return updated_dataset


def upload_the_dataset_if_extra_rows(updated_dataset, database_dataset):

    new_rows = updated_dataset.shape[0]

    if new_rows > database_dataset.shape[0]:
        engine = create_engine(f'mysql+mysqlconnector://root:{password}@localhost:3306/ticketmaster')
        connection = engine.connect()

        updated_dataset.to_sql(name = 'triangle', con = engine, if_exists = 'replace', chunksize = 1000, index=False)

        extra_events = updated_dataset['event id'].nunique() - current_dataset['event id'].nunique()  
        extra_rows = updated_dataset.shape[0] - current_dataset.shape[0]  
        logging.debug(f'This pull resulted in {extra_rows} new rows')
        logging.debug(f'This pull resulted in {extra_events} new events')
        logging.debug(f'Updating mysql was successful:')
        logging.debug(f'The original dataset in mysql had {database_dataset.shape[0]} rows')
        logging.debug(f'The updated dataset in mysql now has {updated_dataset.shape[0]} rows')


    elif new_rows < database_dataset.shape[0]:
        database_dataset.to_csv('ticketmaster_database_dataset_investigate.csv')
        api_dataset.to_csv('ticketmaster_api_dataset_investigate.csv')
        appended_dataset.to_csv('ticketmaster_appended_dataset_investigate.csv')
        updated_dataset.to_csv('ticketmaster_updated_dataset_investigate.csv')
        extra_events = updated_dataset['event id'].nunique() - database_dataset['event id'].nunique()  
        extra_rows = updated_dataset.shape[0] - database_dataset.shape[0]  
        logging.debug(f'This pull resulted in {extra_rows} new rows')
        logging.debug(f'This pull resulted in {extra_events} new events')
        logging.debug('We somehow ended up with fewer rows than before: Exported all the data sets for investigation')


    else:
        logging.debug('This pull resulted in 0 new rows')
        logging.debug('This pull resulted in 0 new events')
        logging.debug('Hence, we did not update the existing dataset in the MySQL database')