import Classes as cl
import pandas as pd
import numpy as np
import sys
# import sklearn
# from sklearn import datasets



# Check if we have customers already or if the current is the only customer
# If there is no csv, there are no customers so we create a file in the except part
try:
    customer_ids = pd.read_csv("customers.csv", usecols=['id'], squeeze=True)
except:
    customer_ids = pd.DataFrame({'name':[],'id':[]})
    customer_ids.to_csv('customers.csv', mode='a', header=True)


# Force the user to enter a correct customer ID.
# It should be an integer
while True:
    id = input("Please enter Customer ID (integer) ")
    if id == 'exit':
        sys.exit()

    try: 
        int(id)
        break
    except:
        print('invalid customer ID. It should be an integer. Enter again or type "exit" to abort')


# https://ddf46429.springboard.com/uploads/resources/1599511103_Python_OOP_Mini_Project_1_.pdf

# Check if the customer already exists or if we need to create a new entry
if int(id) in list(customer_ids):
    while True:
        print('customer exists')
        next_action = input("What action would you like to take for this customer? (options: withdrawal, deposit, apply for credit card, apply for mortgage, compute interest of savings account) ")

        if next_action.UPCASE == 'WITHDRAWAL':
            continue
        elif next_action.UPCASE == 'DEPOSIT':
            continue
        elif next_action.UPCASE == 'APPLY FOR CREDIT CARD':
            continue
        elif next_action.UPCASE == 'APPLY FOR MORTGAGE':
            continue
        elif next_action.UPCASE == 'COMPUTE INTEREST FOR SAVINGS ACCOUNT':
            continue
        elif next_action.UPCASE == 'EXIT':
            sys.exit()
            
        else:
            print("Check your spelling or press exit to abort and start over: (available options: withdrawal, deposit, apply for credit card, apply for mortgage, compute interest of savings account)")

    else:
        print('Not there!')
    #     customers_holder = pd.DataFrame({'name':['John'],'id':[id]})
    #     customers_holder.to_csv('customers.csv', mode='a', header=False)

# with open('my_csv.csv', 'a') as f:
#     df.to_csv(f, header=False)
# https://stackoverflow.com/questions/17530542/how-to-add-pandas-data-to-an-existing-csv-file

# c = Classes.CreditCard(660).application()
# print(c)
# print(addit(1,2))
# print(CreditCard(660).application())
# print(CreditCard(640).application())
# print('\n')
# print(FixeMortgage30(590).application())
# print(FixeMortgage30(680).application())
# print(FixeMortgage30(750).application())
# print(FixeMortgage30(680).interest_rate)
# print(FixeMortgage30(750).interest_rate)