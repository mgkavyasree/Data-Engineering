# part 1: python basics

# factorial using loop
def factorial(n):
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result

# print names with score > 75 and average score
def student_scores():
    data = [("aarav", 80), ("sanya", 65), ("meera", 92), ("rohan", 55)]
    print("students scoring above 75:")
    total = 0
    for name, score in data:
        total += score
        if score > 75:
            print(name)
    avg = total / len(data)
    print("average score:", avg)


# part 2: classes and inheritance

# bankaccount class with deposit and withdraw
class bankaccount:
    def __init__(self, holder_name, balance=0):
        self.holder_name = holder_name
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount
        print("deposit successful. new balance:", self.balance)

    def withdraw(self, amount):
        if amount > self.balance:
            raise Exception("insufficient balance")
        self.balance -= amount
        print("withdraw successful. new balance:", self.balance)

# savingsaccount with interest
class savingsaccount(bankaccount):
    def __init__(self, holder_name, balance=0, interest_rate=0.05):
        super().__init__(holder_name, balance)
        self.interest_rate = interest_rate

    def apply_interest(self):
        interest = self.balance * self.interest_rate
        self.balance += interest
        print("interest applied. new balance:", self.balance)


# part 3: csv task – data cleaning

import pandas as pd

# clean orders.csv
def clean_orders():
    df = pd.read_csv("orders.csv")
    df["CustomerName"].fillna("unknown", inplace=True)
    df["Quantity"].fillna(0, inplace=True)
    df["Price"].fillna(0, inplace=True)
    df["TotalAmount"] = df["Quantity"] * df["Price"]
    df.to_csv("orders_cleaned.csv", index=False)
    print("orders_cleaned.csv saved")


# part 4: json task – data manipulation

import json

# add stock status in inventory.json
def update_inventory():
    with open("inventory.json") as f:
        data = json.load(f)
    for item in data:
        item["status"] = "in stock" if item["stock"] > 0 else "out of stock"
    with open("inventory_updated.json", "w") as f:
        json.dump(data, f, indent=4)
    print("inventory_updated.json saved")


# part 5: enrichment with numpy

import numpy as np

# random scores analysis and save to csv
def generate_scores():
    scores = np.random.randint(35, 101, size=20)
    above_75 = np.sum(scores > 75)
    mean = np.mean(scores)
    std_dev = np.std(scores)
    print("scores:", scores)
    print("students scoring above 75:", above_75)
    print("mean:", mean)
    print("std deviation:", std_dev)
    df = pd.DataFrame({"score": scores})
    df.to_csv("scores.csv", index=False)
    print("scores.csv saved")