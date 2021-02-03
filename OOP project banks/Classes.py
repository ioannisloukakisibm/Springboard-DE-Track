class BankAccount:
    """This is the parent class for all types of bank accounts.
    It contains all the generic actions that can be taken for all specific bank account classes.
    You can open a new account, withdraw or deposit
    """

    def __init__(self, balance = 0):
        self.balance = balance

    def open_new_account(self, balance):
        self.balance = balance
    
    def withdraw(self, amount):
        self.balance -= amount
        return self.balance

    def deposit(self, amount):
        self.balance += amount
        return self.balance


class SavingsAccount(BankAccount):

    """We are using the functionality of the bank account class and adding new functionality specific to Savings accounts.
    We are adding a monthly interest computation 
    """
    def __init__(self, balance, interest_rate = 0.0032):
        BankAccount.__init__(self, balance)
        self.interest_rate = interest_rate

    def compute_monthly_interest(self, n_periods = 1):
        return self.balance * ( (1+self.interest_rate)**(n_periods/12)-1)


class CheckingAccount(BankAccount):
    """We are using the functionality of the bank account class and adding new functionality specific to checking accounts.
    We are adding the ability to conditionally (based on balance) order a new checkbook
    """
    def __init__(self, balance):
        BankAccount.__init__(self, balance)

    def apply_for_checkbook(self):
        if self.balance < 100:
            return "your balance is less than $100. Add more funds to qualify for a checkbook" 
        else:
            return "Your checkbook will be on its way soon!" 




class Customer():
    """We are creating a customer class to be able to update our customer database when registering a new customer"""

    def __init__(self, first_name, last_name, ssn):
        self.first_name = first_name
        self.last_name = last_name
        self.ssn = ssn


class FixeMortgage30():

    """This is a mortgage class.
    We can use it to conditionally evaluate mortgage applications and determine the interest rate based on credit score
    """

    def __init__(self, credit_score, interest_rate=0.043):
        self.interest_rate = interest_rate
        self.credit_score = credit_score
        if (self.credit_score > 600) & (self.credit_score < 690):
            self.interest_rate = 0.075 

    def application(self, outcome = 'Approved'):
        self.outcome = outcome
        if self.credit_score <= 600:
            self.outcome = 'Denied' 
        return self.outcome


class CreditCard():

    """
    This is a credit card class.
    We can use it to conditionally evaluate credit card applications based on credit score
    """

    def __init__(self, credit_score, interest_rate=0.25):
        self.interest_rate = interest_rate
        self.credit_score = credit_score

    def application(self, outcome = 'Approved'):
        self.outcome = outcome
        if self.credit_score < 650:
            self.outcome = 'Denied' 
        return self.outcome


