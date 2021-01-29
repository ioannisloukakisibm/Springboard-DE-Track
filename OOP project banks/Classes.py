class BankAccount:
    def __init__(self, balance = 0):
        self.balance = balance
    
    def withdraw(self, amount):
        self.balance -= amount
        return self.balance

    def deposit(self, amount):
        self.balance += amount
        return self.balance


class SavingsAccount(BankAccount):
    def __init__(self, balance, interest_rate):
        BankAccount.__init__(self, balance)
        self.interest_rate = interest_rate

    def compute_interest(self, n_periods = 1):
        return self.balance * ( (1+self.interest_rate)**n_periods-1)


class Customer():
    def __init__(self, first_name, last_name):
        self.first_name = first_name
        self.last_name = last_name


class FixeMortgage30():
    def __init__(self, credit_score, interest_rate=0.05):
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
