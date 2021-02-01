import Classes as cl
import pandas as pd
import numpy as np
import sys
import random 

# import sklearn
# from sklearn import datasets


# Check if we have customers already or if the current is the only customer
# If there is no csv, there are no customers so we create a file in the except part
try:
    customer_ids = pd.read_csv("customers.csv", usecols=['id'], squeeze=True)
    customer_ssns = pd.read_csv("customers.csv", usecols=['ssn'], squeeze=True)
except:
    customer_set = pd.DataFrame({'id':[55],'ssn':[999999999],'first_name':['h'],'last_name':['h']})
    customer_set.to_csv('customers.csv', mode='a', header=True)
    customer_ids = pd.read_csv("customers.csv", usecols=['id'], squeeze=True)
    customer_ssns = pd.read_csv("customers.csv", usecols=['ssn'], squeeze=True)


print(" Good morning! What's next? \n Enter the appropriate number of the following options: \n 1 withdrawal\n 2 deposit\n 3 apply for credit card\n 4 apply for mortgage\n 5 add a new customer")

while True:
    action = input()

    if action.upper() == 'EXIT':
        sys.exit()
    
    try:
        action = int(action)

        if (action<=6) & (action>=1):
            break
        else:
            print("please enter the appropriate integer number from the list above. type 'exit' to abort")
    except:
        print("please enter the appropriate integer number from the list above. type 'exit' to abort")



if action == 1:
    print('wip')
elif action == 2:
    print('wip')

elif action == 3:
    print("Enter the customer's credit score or type 'exit' to abort ")

    while True:
        credit_score = input()

        if credit_score.upper() == 'EXIT':
            sys.exit()
        
        try:
            credit_score_length = len(credit_score)
            credit_score = int(credit_score)

            if (credit_score_length==3) & (credit_score<800) & (credit_score>100):
                break
            else:
                print("please enter the correct customer's credit score. It should be a 3-digit integer between 100 and 800. Type 'exit' to abort ")
        except:
            print("please enter the correct customer's credit score. It should be a 3-digit integer between 100 and 800. Type 'exit' to abort ")

    credit_card = cl.CreditCard(credit_score)
    outcome = credit_card.application()
    interest_rate = credit_card.interest_rate

    if outcome == "Approved":
        print(f"The credit card application was {outcome} with an interest rate of {interest_rate:.2%}")
    else:
        print(f"The credit card application was {outcome}")

elif action == 4:
    print("Enter the customer's credit score or type 'exit' to abort ")

    while True:
        credit_score = input()

        if credit_score.upper() == 'EXIT':
            sys.exit()
        
        try:
            credit_score_length = len(credit_score)
            credit_score = int(credit_score)

            if (credit_score_length==3) & (credit_score<800) & (credit_score>100):
                break
            else:
                print("please enter the correct customer's credit score. It should be a 3-digit integer between 100 and 800. Type 'exit' to abort ")
        except:
            print("please enter the correct customer's credit score. It should be a 3-digit integer between 100 and 800. Type 'exit' to abort ")

    mortgage = cl.FixeMortgage30(credit_score)
    outcome = mortgage.application()
    interest_rate = mortgage.interest_rate

    if outcome == "Approved":
        print(f"The mortgage application was {outcome} with an interest rate of {interest_rate:.2%}")
    else:
        print(f"The mortgage application was {outcome}")


elif action == 5:
    print("Enter the customer's social security number with no dashes or type 'exit' to abort ")

    while True:
        ssn = input()

        if ssn.upper() == 'EXIT':
            sys.exit()
        
        try:
            ssn_length = len(ssn)
            ssn = int(ssn)
            if ssn in list(customer_ssns):
                print("Customer already exists. Enter a different SSN or type 'exit' to abort ")
                continue

            if ssn_length==9:
                break
            else:
                print("please enter the correct customer's social security number with no dashes. It should be 9 digits. type 'exit' to abort ")
        except:
            print("please enter the correct customer's social security number with no dashes. It should be 9 digits. type 'exit' to abort ")
            
            

    while True:
        first_name = input("Enter the customer's first name or type 'exit' to abort ")

        if first_name.upper() == 'EXIT':
            sys.exit()
        
        try:
            first_name = int(first_name)
            print("please enter customer's first name. This should not be a number. type 'exit' to abort ")
        except:
            break

    while True:
        last_name = input("Enter the customer's last name or type 'exit' to abort ")

        if last_name.upper() == 'EXIT':
            sys.exit()
        
        try:
            last_name = int(last_name)
            print("please enter customer's last name. This should not be a number. type 'exit' to abort ")
        except:
            break


    while True:
        id = random.randint(1,10000000000)
        if id in list(customer_ids):
            continue        
        else:
            break


    customer_holder = cl.Customer(first_name,last_name,ssn)
    new_customer = pd.DataFrame({'id':[id],'ssn':[ssn],'first_name':[first_name],'last_name':[last_name]})
    new_customer.to_csv('customers.csv', mode='a', header=False)
    print("customer registered successfully!")














# # https://ddf46429.springboard.com/uploads/resources/1599511103_Python_OOP_Mini_Project_1_.pdf

# # Check if the customer already exists or if we need to create a new entry
# if int(id) in list(customer_ids):
#     #     customers_holder = pd.DataFrame({'name':['John'],'id':[id]})
#     #     customers_holder.to_csv('customers.csv', mode='a', header=False)

# # with open('my_csv.csv', 'a') as f:
# #     df.to_csv(f, header=False)
# # https://stackoverflow.com/questions/17530542/how-to-add-pandas-data-to-an-existing-csv-file

# # c = Classes.CreditCard(660).application()
# # print(c)
# # print(addit(1,2))
# # print(CreditCard(660).application())
# # print(CreditCard(640).application())
# # print('\n')
# # print(FixeMortgage30(590).application())
# # print(FixeMortgage30(680).application())
# # print(FixeMortgage30(750).application())
# # print(FixeMortgage30(680).interest_rate)
# # print(FixeMortgage30(750).interest_rate)