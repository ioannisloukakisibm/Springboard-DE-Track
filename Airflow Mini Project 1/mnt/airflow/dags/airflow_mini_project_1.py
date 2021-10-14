import pandas as pd
import numpy as np

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

t0>>t1>>t3
t0>>t2>>t4
t3>>t5
t4>>t5

# https://storage.googleapis.com/sp-springboard-resources-uploaded-images/uploads/resources/1619197358_SoDA_DEC_-_Airflow_Mini_Project__DAG_Exercise.pdf?Expires=1629206137&GoogleAccessId=storage-admin%40springboard-production.iam.gserviceaccount.com&Signature=pCv13dTLvQPfLn2OG1KvUzlk7lWRFAIWDurTmPVxqhLOYZOt0hnsRwK6P%2BEH7Mu8AyxNxgY2aIvO86Bbj9sHOsRH%2Be4JYTgCvCGMkCnjTHI3AW5DzYTmCqI5W29%2Fw%2F8VWxkpyWpRT1eGQS66SC9i1YN5VGIut6r6O70OyM%2FpKbZDoThXu8ZXk0K1UGRY7sLDD%2FWfckt6Tq4oXuVewsiRiBmy4cIgDVXT1NseWzBOqjzcQ0VCIiqJZmA%2B9KwfBTgZ2SP%2FAFO44F1HM8G3UheFavb7PK904qw4Ci4VKvHnbHA7bQdZ1cUWie6NaNQjqpBv7Ggmn89VIWsRjMBztGRqSg%3D%3D
# https://storage.googleapis.com/sp-springboard-resources-uploaded-images/uploads/resources/1619197674_SoDA_DEC_-_Airflow_Mini_Project__Log_analyzer.pdf?Expires=1629308946&GoogleAccessId=storage-admin%40springboard-production.iam.gserviceaccount.com&Signature=vBxJ9AvE7zBqWXWhWmeELsJXw839eDA8uw7mWrZJ1MLrkPEKro9aCQaQkGqLB7g%2Bt2MdxmkwYNV3sdSf7vdVQGll06%2BAbY%2F6l2iHk%2FsgWyTy0rR0zVUZWFNWickG53CXFn%2B7yo4SC%2FkGGaNcPoklN3wEerD6Ei%2BoKjpsszNnCHbpHd%2FJvxMP49K3Yg8WRWKoIv38sB153AdzNsaem1WhpMLkuI6SYOJq87GkibkMqH15NtJ5SD%2FZwW67mj21OGmXEXIUVZeeplL8Mrjn9of3zN1nS9yZCcalE%2FD%2BmMXHdoshXegjNXT6z6Ka4%2BUTUP2WDlh%2FsdNZQCHhYyeoolviWw%3D%3D