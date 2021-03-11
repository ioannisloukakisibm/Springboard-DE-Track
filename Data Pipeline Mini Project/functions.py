import mysql
from decouple import config
import mysql.connector
from sqlalchemy import *
import pandas as pd

password = config('mysql_password')

# Connect to the database function
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

def query_most_expensive_ticket(connection):
    sql_statement = "select max(price) from data_pipeline_mini_project.tickets"
    cursor=connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()

    # Retrive the information from the data structure
    record = records[0]
    record = record[0]

    return record


def query_most_popular_event(connection):
    sql_statement = "select distinct event_name from data_pipeline_mini_project.tickets where num_tickets = (select max(num_tickets) from data_pipeline_mini_project.tickets)"
    cursor=connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()

    return records


def query_most_popular_event_type(connection):
    sql_statement = \
    """
        with grouped_set as  
            (select event_type, count(event_type) as row_count from data_pipeline_mini_project.tickets group by event_type) 
        
        select event_type from grouped_set
            where row_count = (select max(row_count) from grouped_set)
    """
    cursor=connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()

    return records
