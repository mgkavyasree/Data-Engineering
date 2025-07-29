# part 1: basics

# check if a number is prime
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True

# reverse a string and check if it's a palindrome
def check_palindrome():
    s = input("enter a string: ")
    reversed_s = s[::-1]
    print("reversed string:", reversed_s)
    if s == reversed_s:
        print("it's a palindrome")
    else:
        print("not a palindrome")

# remove duplicates, sort list, find second largest
def second_largest_unique(numbers):
    unique = sorted(set(numbers))
    if len(unique) >= 2:
        print("second largest number:", unique[-2])
    else:
        print("not enough unique numbers")


# part 2: classes and inheritance

# person and employee classes
class person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def display(self):
        print("name:", self.name, "age:", self.age)

class employee(person):
    def __init__(self, name, age, employee_id, department):
        super().__init__(name, age)
        self.employee_id = employee_id
        self.department = department

    def display(self):
        print("name:", self.name, "age:", self.age, "id:", self.employee_id, "department:", self.department)

# vehicle and car classes with method override
class vehicle:
    def drive(self):
        print("driving a vehicle")

class car(vehicle):
    def drive(self):
        print("driving a car safely")


# part 3: csv and json handling

import pandas as pd
import numpy as np
import json

# clean students.csv
def clean_csv():
    df = pd.read_csv("students.csv")
    df["Age"].fillna(df["Age"].mean(), inplace=True)
    df["Score"].fillna(0, inplace=True)
    df.to_csv("students_cleaned.csv", index=False)
    print("students_cleaned.csv saved")

# convert cleaned csv to json
def convert_to_json():
    df = pd.read_csv("students_cleaned.csv")
    df.to_json("students.json", orient="records", indent=4)
    print("students.json saved")


# part 4: data cleaning and transformation

# add status and tax_id columns
def transform_data():
    df = pd.read_csv("students_cleaned.csv")
    df["Status"] = np.where(df["Score"] >= 85, "distinction",
                    np.where(df["Score"] >= 60, "passed", "failed"))
    df["Tax_ID"] = df["ID"].apply(lambda x: f"tax-{int(x)}")
    df.to_csv("students_transformed.csv", index=False)
    print("students_transformed.csv saved")


# part 5: json manipulation

# increase prices in products.json by 10%
def update_prices():
    with open("products.json") as f:
        data = json.load(f)
    for item in data:
        item["price"] = round(item["price"] * 1.10, 2)
    with open("products_updated.json", "w") as f:
        json.dump(data, f, indent=4)
    print("products_updated.json saved")