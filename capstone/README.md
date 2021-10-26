

I need a configuration for postgress in airflow to know where the postgres is
Add a healthcheck to determine if its running.
Make sure Airflow's metadata is in postgress

## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [LocalExecution](#localexecution)

## General Info
This is a school project organized for the author to experiment on docker technologies/Airflow/APIs and has no value to society whatsoever. 

## Description
In this project, a weekly automatic batch of about 10K rows is being pulled from the Spotify API and stored into a local instance of a MYSQL database. The songs might be the same or different in each week. Spotify is providing a big number of very detailed metrics in each song. The plan is to use these metrics to predict whether newly-released songs will become popular.

## Technologies
Project is created with:
* Docker
* Apache Airflow
* Python 3.8+


## LocalExecution

If for some strange reason someone wants to clone this repo, all you have to do is run the main program as seen below to execute the program locally (ie not deployed using docker or airflow, but run manually). CAVEAT: You need to install a local instance of a MYSQL database and update the credentials in the code, as well as apply for the Spotify API credentials (Free) and update these in the code as well. 

'''
python main.py
'''

