# Ticketing Engine

This repository contains all the source code for the backend implementation of a ticketing system interphase. This interphase was designed to be used by the public trying to get a better idea of the general event/ticket popularity and pricing. In the paragraphs below, you can find a detailed description of the code in this repository and how to use it. 


### Code overview:

- functions.py

This is the code housing all the functions we use to implement the user actions. These actions include looking for the most expensive ticket, the name of the most popular event and the name of the most popular event type. If two or more events or event types are equally popular, they will all be displayed. These functions are imported and used in the display_statistical_information.py program--the main program in this repository. 

- display_statistical_information.py

This code is designed for the user interaction with the system. It is a series of try-except statements, while loops and conditionals designed to prevent the user from entering wrong information. In each step, the user can type '4' to abort and get out of the program. At the end of each code block, we use the functions created in the functions.py program to display the desired results.

- upload_data.py

This is a one-off code designed to connect to the local database and upload the csv of the data in the local server.


### How to run the interphase

Point your terminal of choice to the location this repository is stored in your system. This is a python designed interphase, so make sure you have [python](https://www.python.org/downloads/) installed. Use the command line to type the following:

display_statistical_information.py

hit 'enter' and you should be prompted to take actions. Follow the self-explanatory directions. type '4' at any time to abort and start over.

