# import the libraries
# There are so many parts in sqlalchemy that you might as well import all of them
# Import the functions from functions.py
import mysql
from decouple import config
import mysql.connector
from sqlalchemy import *
import pandas as pd
from functions import *


conn = get_db_connection()

while True:
    selected_number = input("""
    please type:  
    1) for the most expensive ticket  
    2) for the most popular event(s)  
    3) for the most popular event type(s) 
    4) to abort
    """)


    try:
        selected_number = int(selected_number)

    except:
        print("Enter a valid INTEGER. type 4 to abort")
        continue
 
    if selected_number == 1:
        most_expensive_ticket = query_most_expensive_ticket(conn)
        print('The highest price of all tickets is', most_expensive_ticket)
        break


    elif selected_number == 2:
        most_popular_events = query_most_popular_event(conn)

        print('Most popular event(s):')
        for most_popular_event in most_popular_events: 
            print(most_popular_event[0])
        break


    elif selected_number == 3:
        most_popular_event_types = query_most_popular_event_type(conn)

        print('Most popular event type(s):')
        for popular_event_type in most_popular_event_types: 
            print(popular_event_type[0])
        break

    elif selected_number == 4:
        break

    else:
        print("Value needs to be an integer between 1 and 3. No decimals or strings please. Type 4 to abort")




