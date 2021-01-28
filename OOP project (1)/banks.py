class BankAccount:
    def __init__(self):
        self.balance = 0
    
    def withdraw(self, amount):
        self.balance -= amount
        return self.balance

    def deposit(self, amount):
        self.balance += amount
        return self.balance


b = BankAccount().deposit(100)
print (b)
print ("hello")
opatis!
