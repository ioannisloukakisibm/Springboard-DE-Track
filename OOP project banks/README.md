
# Banking System

This repository contains all the source code for the backend implementation of a banking system interphase. This interphase was designed to be used by bankers trying to either enter new customers in their system or help existing customers apply for financial products (Bank accounts, credit cards, mortgages) or make withdrawals and deposits to existing accounts. In the paragraphs below, you can find a detailed description of the code in this repository and how to use it. 


### Code overview:

- Classes.py

This is the code housing all the classes that we use to implement the user actions. These actions are the ones described in the overview above (apply for mortgages/credit cards, open bank accounts etc). These classes are imported and used in the drive.py program--the main program in this repository 

- drive.py
The rest of the code is designed for the user interaction with the system. 
It is a series of try-except statements, while loops and conditionals designed to prevent the user from entering wrong information.
In each step, the user can type 'exit; to abort and get out of the program.
At the end of each code block, we use the classes created in the Classes.py program to populate our database (csv) 
with the new information entered by the user.
"""

### How to run the interphase
- Let's see if this works

### Where is the data stored
customer.csv is our customer database. In the following try - except bblock We first Check if we have customers already or if the current is the only customer.
If there is no csv, we assume there are no customers so we create a file with all the columns and a fake row
"""
