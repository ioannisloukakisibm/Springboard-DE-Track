import pandas as pd
import numpy as np

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

# Ignore harmless warnings
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime, timedelta, date

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import yfinance as yf

default_args = {
    'start_date':'2021-08-17'
    ,'schedule_interval':'0 18 * * 1-5'
    ,'retries': 2
    ,'retry_delay': timedelta(minutes=5)
}

dag = DAG(
'marketvol',
default_args=default_args,
description='A simple DAG',
)

today = date.today()
today = today.strftime("%y-%m-%d")

t0 = BashOperator(task_id = 'create_directory'
                    ,bash_command = f'mkdir -p /tmp/data/{today}'
                    ,dag = dag
                    )

def stock(stock_symbol):
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    df = yf.download(stock_symbol, start=start_date, end=end_date, interval='1m')
    df.to_csv(f"{stock_symbol}_data.csv", header=False)
    return None


t1 = PythonOperator(
    task_id = 'download_apple'
    ,python_callable = stock('AAPL')
    ,dag=dag
)

t2 = PythonOperator(
    task_id = 'download_tesla'
    ,python_callable = stock('TSLA')
    ,dag=dag
)

t3 = BashOperator(task_id = 'move_apple'
                    ,bash_command = f'AAPL_data.csv /tmp/data/{today}'
                    ,dag = dag
                    )

t4 = BashOperator(task_id = 'move_tesla'
                    ,bash_command = f'TSLA_data.csv /tmp/data/{today}'
                    ,dag = dag
                    )


def query_apple_tesla():
    apple = pd.read_csv(f'/tmp/data/{today}/APPL_data.csv')
    tesla = pd.read_csv(f'/tmp/data/{today}/TSLA_data.csv')
    return None


t5 = PythonOperator(
    task_id = 'query_data'
    ,python_callable = query_apple_tesla()
    ,dag=dag
)
