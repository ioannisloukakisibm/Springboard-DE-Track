import requests
import json
import docx
import pandas as pd
import numpy as np
from datetime import datetime
from decouple import config

import mysql
import mysql.connector
from sqlalchemy import *

api_key = config('ticketmaster_apikey')
password = config('mysql_password')

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
    print(response)
    data=response.text

    parsed = json.loads(data)

    # Combine all the events
    try:
        list_of_triangle_events.extend(parsed.get('_embedded').get('events'))
        index+=1
    except:
        print("successfully pulled",index-1,"Triangle pages and",len(list_of_triangle_events), "events")
        break

list_of_triangle_events.extend(list_of_carolina_theater_events)

print("We added", len(list_of_carolina_theater_events), "Carolina Theater events for a total of" \
, len(list_of_triangle_events), "Triangle events")



# GET EVENT INFO
# Put all the events in a dataframe
events = pd.DataFrame({'events':list_of_triangle_events})

# Break the disctionaries into columns
# Now we have 1 column for each sales, locale, dates, etc
iteration_1 = events['events'].apply(pd.Series)

# Get the event names and add them to a dataframe
list_of_event_names = [event.get('name') for event in list_of_triangle_events]
list_of_event_ids = [event.get('id') for event in list_of_triangle_events]
test = [event.get('test') for event in list_of_triangle_events]

event_names_df = pd.DataFrame({'event name':list_of_event_names, 'event id':list_of_event_ids, 'is this entry a test?':test})


# VENUES
embedded_df = iteration_1._embedded.apply(pd.Series)

list_of_venue_dictionaries = [embedded_df.venues[index][0] for index in range(len(embedded_df.venues))] 

venue_df = pd.DataFrame({'venue_dictionaries':list_of_venue_dictionaries})
venue_final = pd.DataFrame(venue_df['venue_dictionaries'].apply(pd.Series)['name'])
venue_final.rename({'name': 'venue name'}, axis = 1, inplace = True)

# SALES
pd.set_option('display.max_colwidth',500)

# Break the sales column into columns
# We now have public sales and presales
sales = iteration_1.sales.apply(pd.Series)

# Break the public sales into columns
sales_public = sales.public.apply(pd.Series)

# rename the public sales columns to distinguish them from the rest
for col in sales_public.columns:
    sales_public.rename({col:f'public sales {col}'}, axis = 1, inplace = True)
    


# Create a dataframe to add the final product of the steps below
presale_final = pd.DataFrame()

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
        presale_final = pd.concat([presale_final,dict_holder_df],axis=0)
        continue

# """
# The dictionaries will contain the name of the presale as well as the end and start date. 
# We need to extract the name and move it to the column names so that we can distinguish the start and end dates of each presale
# """
    dict_holder_df = pd.DataFrame()
    for presale_dict in presales_list_element:
        
        start_time_title = presale_dict['name'] + ' ' + list(presale_dict.keys())[0]
        end_time_title = presale_dict['name'] + ' ' + list(presale_dict.keys())[1]
        
        holder_df = pd.DataFrame({start_time_title:[presale_dict['startDateTime']], end_time_title:[presale_dict['endDateTime']]})
        dict_holder_df = pd.concat([dict_holder_df,holder_df], axis=1)
        
    presale_final = pd.concat([presale_final,dict_holder_df],axis=0)


# Reset the index so that we can merge
# drop the blank column
presale_final.reset_index(inplace=True)
presale_final.drop(columns=['index', 'blank'], inplace=True)


# DATES
pd.set_option('display.max_colwidth',500)

# Break the dates column into columns
# There are a lot of dates trapped into disctionaries. Use pd.Series multiple times to untangle everything
dates = iteration_1.dates.apply(pd.Series)
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


# CLASSIFICATIONS
pd.set_option('display.max_colwidth',500)

list_of_classification_dictionaries = [iteration_1.classifications[index][0] for index in range(len(iteration_1.classifications))] 

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


# PROMOTER
# Break the promoters column into columns
promoters = iteration_1.promoters.apply(pd.Series)

for index in range(len(promoters.columns)):
    promoters.rename({promoters.columns[index]: 'event promoter' + ' ' + str(promoters.columns[index]+1)}, axis = 1, inplace = True)

promoters_df_final = pd.DataFrame()

for column in promoters.columns:
    series_holder = promoters[column].apply(pd.Series)
    series_holder = series_holder[['name', 'description']]
    series_holder.rename({'name':column + ' ' + 'name', 'description':column + ' ' + 'description'}, inplace = True, axis = 1)
    promoters_df_final = pd.concat([promoters_df_final,series_holder], axis = 1)

# PRICE 
# Sometimes the price information is missing from an event. 
# Replace NA with the word missing
iteration_1.fillna({'priceRanges':'missing'}, inplace=True)


# create a genberic pricing structure to replace the missing pricing information
# set all prices to zero
generic_pricing_structure = [{'type': 'standard', 'currency': 'USD', 'min': -1, 'max': -1}]        

# Unfortunately the replace will get rid of the list part and replace everything with a 'naked' dictionary
# Ideally we would have a list with the generic pricing dictionary as its only element
test = iteration_1.replace({'missing':generic_pricing_structure})


# The try-except step will fix this issue. 
# If we have a list with a dictionary inside, we will fetch the dictionary from inside the list
# Otherwise we will fetch the dictionary directly 
list_of_pricerange_dictionaries = []

for i, row in test.iterrows():
    try:
        x = row['priceRanges'].get('type')
        list_of_pricerange_dictionaries.append(row['priceRanges'])
    except:
        list_of_pricerange_dictionaries.append(row['priceRanges'][0])

        
pricerange_df = pd.DataFrame({'pricerange_dictionaries':list_of_pricerange_dictionaries})
pricerange_df_broken_in_columns = pricerange_df['pricerange_dictionaries'].apply(pd.Series)


pricerange_df_broken_in_columns = pricerange_df_broken_in_columns[['type', 'min', 'max']]
pricerange_df_broken_in_columns.rename({'type':'price type', 'min':'price min', 'max':'price max'}, axis = 1, inplace = True)


# TICKET LIMIT 
# iteration_1.columns
ticket_limit = pd.DataFrame(iteration_1['ticketLimit'].apply(pd.Series))
ticket_limit = pd.DataFrame(ticket_limit['info']) 
ticket_limit.rename({'info':'ticket limit'},axis = 1, inplace = True)
please_note = pd.DataFrame(iteration_1['pleaseNote'])


# MERGE
new_dataset = pd.concat([
    event_names_df
    ,venue_final
    ,iteration_1['info']
    ,sales_public
    ,presale_final
    ,start_dates
    ,status
    ,initial_start_date
    ,classification_df_final
    ,promoters_df_final
    ,pricerange_df_broken_in_columns
    ,ticket_limit
    ,please_note
    
], axis = 1)

list_of_groupby_columns = list(new_dataset.columns)

datatypes_df = pd.DataFrame(new_dataset.dtypes, columns = ['data type']).reset_index()

list_of_string_variables = list(datatypes_df[datatypes_df['data type'] == 'object']['index'])
list_of_boolean_variables = list(datatypes_df[datatypes_df['data type'] == 'bool']['index'])
list_of_numeric_variables = list(datatypes_df[datatypes_df['data type'] == 'float']['index'])


for var in list_of_groupby_columns:
    if var in list_of_string_variables:
        new_dataset.fillna({var: '-1'}, inplace = True)
    else:
        new_dataset.fillna({var: -1}, inplace = True)
        

today_timestamp = pd.to_datetime("today")
today_date = today_timestamp.date()
new_dataset['timestamp of data pull'] = today_timestamp
new_dataset['date of data pull'] = today_date


# Connect to the database function
def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            user='root'
            ,password=f'{password}' 
            ,host='localhost' 
            ,port='3306' 
            ,database='ticketmaster')

    except Exception as error:
        print("failed to connect", error)

    return connection

connection = get_db_connection()

sql_statement = "select * from ticketmaster.ticketmaster_triangle"
cursor=connection.cursor()
cursor.execute(sql_statement)
records = cursor.fetchall()
cursor.close()

current_dataset = pd.DataFrame(records, columns = list(cursor.column_names))

try:
    appended_dataset = pd.concat([current_dataset, new_dataset], axis = 0)
    # appended_dataset.drop(columns = [appended_dataset.columns[0]], inplace = True)

    appended_dataset['timestamp of data pull'] = pd.to_datetime(appended_dataset['timestamp of data pull'])
    appended_dataset['date of data pull'] = pd.to_datetime(appended_dataset['date of data pull'])

    updated_dataset = appended_dataset.groupby(list_of_groupby_columns, as_index = False) \
    .agg({'date of data pull':'min', 'timestamp of data pull':'min'}) 

    engine = create_engine(f'mysql+mysqlconnector://root:{password}@localhost:3306/ticketmaster')
    connection = engine.connect()

    updated_dataset.to_sql(name = 'ticketmaster_triangle', con = engine, if_exists = 'replace', chunksize = 1000, index=False)

    extra_events = updated_dataset['event id'].nunique() - current_dataset['event id'].nunique()  
    extra_rows = updated_dataset.shape[0] - current_dataset.shape[0]  
    print ('This pull resulted in', extra_rows, 'new rows')
    print ('This pull resulted in', extra_events, 'new events')
    print ('The original dataset in mysql had', current_dataset.shape[0], 'rows')
    print ('The updated dataset in mysql now has', updated_dataset.shape[0], 'rows')


except:
    updated_dataset.to_sql(name = 'ticketmaster_triangle', con = engine, if_exists = 'replace', chunksize = 1000, index=False)





