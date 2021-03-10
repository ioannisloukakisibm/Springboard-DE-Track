# import the libraries
# There are so many parts in sqlalchemy that you might as well import all of them
import mysql
from decouple import config
import mysql.connector
from sqlalchemy import *
import pandas as pd


# Import the password from the local .env file
password = config('mysql_password')


# Connect to the database function
# This function is not used in this module
def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            user='root'
            ,password=f'{password}' 
            ,host='localhost' 
            ,port='3306' 
            ,database='data_pipeline_mini_project')

    except Exception as error:
        print("failed to connect", error)

    return connection


# The combination of these two links
# https://docs.sqlalchemy.org/en/14/core/engines.html#mysql
# https://stackoverflow.com/questions/53024891/modulenotfounderror-no-module-named-mysqldb/54031440

# Use the create_engine function to connect to the database
# Generic example:
# dialect+driver://username:password@host:port/database
engine = create_engine(f'mysql+mysqlconnector://root:{password}@localhost:3306/data_pipeline_mini_project')
connection = engine.connect()


# We do not have a table in the schema yet
# We are creating an empty table with the desired columns and treir datatypes
meta = MetaData()

test = Table('tickets', meta,
    Column('ticket_id', Integer, primary_key=True)
    ,Column('trans_date', String(15))
    ,Column('event_id', Integer)
    ,Column('event_name', String(50))
    ,Column('event_date', Date)
    ,Column('event_type', String(10))
    ,Column('event_city', String(20))
    ,Column('customer_id', Integer)
    ,Column('price', Float)
    ,Column('num_tickets', Integer)
)
test.create(engine)

# Check if the table was created
print(engine.table_names())


# Bring in the csv file that we need to upload
# It does not have headers so we ask to create the default (1,2,3... etc)
df = pd.read_csv('third_party_sales_1.csv', header = None)


# Change the headers into the ones that exist in the empty table we created above
new_headers = ['ticket_id', 'trans_date','event_id','event_name','event_date','event_type','event_city', 'customer_id'
,'price','num_tickets']

df.columns = new_headers

# Convert the dates to datetime to match the data type compatibility
df['trans_date'] = pd.to_datetime(df['trans_date'])
df['event_date'] = pd.to_datetime(df['event_date'])


# Upload the dataset
Insert whole DataFrame into MySQL
df.to_sql(name = 'tickets', con = engine, if_exists = 'append', chunksize = 1000, index=False)


# Close the connection
connection.close()

