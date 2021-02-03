
# Banking System

This repository contains all the source code for the backend implementation of a banking system interphase. This interphase was designed to be used by bankers trying to either enter new customers in their system or help existing customers apply for financial products (Bank accounts, credit cards, mortgages) or make withdrawals and deposits to existing accounts. In the paragraphs below, you can find a detailed description of the code in this repository and how to use it. 


### Code overview:

- Classes.py

This is the code housing all the classes that we use to implement the user actions. These actions are the ones described in the overview above (apply for mortgages/credit cards, open bank accounts etc). These classes are imported and used in the drive.py program--the main program in this repository 

- drive.py

This code is designed for the user interaction with the system. It is a series of try-except statements, while loops and conditionals designed to prevent the user from entering wrong information. In each step, the user can type 'exit' to abort and get out of the program. At the end of each code block, we use the classes created in the Classes.py program to populate our database (csv) 
with the new information entered by the user.

### How to run the interphase

Point your terminal of choice to the location this repository is stored in your system. This is a python designed interphase, so make sure you have [python](https://www.python.org/downloads/) installed. Use the command line to type 

python drive.py

You should be prompted to take actions. Follow the self-explanatory directions

### Where is the data stored
customer.csv is our customer database. The program will automatically create this csv locally with all the relevant columns the first time the program runs. If there is no csv present, the assumption is that there are no customers so a file with all the columns and a fake row is created. All new customers are appended to that file automatically based on user input. 

