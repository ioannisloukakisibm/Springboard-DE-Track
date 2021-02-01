class BankAccount:
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
    def __init__(self, balance, interest_rate = 0.0032):
        BankAccount.__init__(self, balance)
        self.interest_rate = interest_rate

    def compute_monthly_interest(self, n_periods = 1):
        return self.balance * ( (1+self.interest_rate)**(n_periods/12)-1)


class CheckingAccount(BankAccount):
    def __init__(self, balance):
        BankAccount.__init__(self, balance)

    def apply_for_checkbook(self):
        if self.balance < 100:
            return "your balance is less than $100. Add more funds to qualify for a checkbook" 
        else:
            return "Your checkbook will be on its way soon!" 




class Customer():
    def __init__(self, first_name, last_name, ssn):
        self.first_name = first_name
        self.last_name = last_name
        self.ssn = ssn


class FixeMortgage30():
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
    def __init__(self, credit_score, interest_rate=0.25):
        self.interest_rate = interest_rate
        self.credit_score = credit_score

    def application(self, outcome = 'Approved'):
        self.outcome = outcome
        if self.credit_score < 650:
            self.outcome = 'Denied' 
        return self.outcome


# c = Customer('John', 'Smith', '432859239')
# print(c.ssn)
# c.ssn = 5
# print(c.ssn)

# s = SavingsAccount(1000, 0.05)
# s.withdraw(500)
# print(s.balance)
# print(s.compute_interest())

# s = SavingsAccount(900, 0.05)
# s.withdraw(1500)
# print(s.balance)

# print(CreditCard(660).application())
# print(CreditCard(640).application())
# print('\n')
# print(FixeMortgage30(590).application())
# print(FixeMortgage30(680).application())
# print(FixeMortgage30(750).application())
# print(FixeMortgage30(680).interest_rate)
# print(FixeMortgage30(750).interest_rate)
