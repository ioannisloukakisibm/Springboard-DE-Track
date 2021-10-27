

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


```
python main.py
```

## Folder description

* airflow: Contains the dag for the project as well as the airflow configuration file
* scripts: It contains the 3 different scripts (data pull, preprocessing, modeling). Each script has 2 versions. 1 with the suffix "airflow" which is used in the airflow deployment and 1 that is used when the pipeline is run locally using the main.py file.  
* files_not_in_use contains all the files that could be of some value for other projects in the future. This project started by targeting the Ticketmaster and Eventbrite APIs. The scope changed and moved focus into the Spotify API for ease of use. The work already done in previous attempts could be useful for other projects and is only valuable as recordkeeping. It has nothing to do with this current project. 


## Future Improvements
* Work on the configuration for postgress in airflow to know where the postgres is. Add a health check and make sure Airflow's metadata is in postgress
* Figure out the memory issue and remove the "limit 420000" from the code to use the entire dataset. 
* Add a dashboarding solution to display the results 
