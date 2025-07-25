import math
import random
import datetime

contacts = {}

# 1. BMI Calculator
def bmi_calculator():
    weight = float(input("Enter your weight in kg: "))
    height = float(input("Enter your height in meters: "))
    bmi = weight / math.pow(height, 2)

    print(f"Your BMI is: {bmi:.2f}")
    if bmi < 18.5:
        print("You are Underweight.")
    elif 18.5 <= bmi <= 24.9:
        print("You are Normal weight.")
    else:
        print("You are Overweight.")

# 2. Strong Password Checker
def strong_password_checker():
    while True:
        pwd = input("Enter a strong password: ")
        has_upper = any(char.isupper() for char in pwd)
        has_digit = any(char.isdigit() for char in pwd)
        has_special = any(char in "!@#$" for char in pwd)

        if has_upper and has_digit and has_special:
            print("Password is strong.")
            break
        else:
            print("Password must contain at least 1 uppercase, 1 number, and 1 special character (!@#$). Try again.")

# 3. Weekly Expense Calculator
def weekly_expense_calculator():
    expenses = []
    print("Enter your expenses for 7 days:")
    for i in range(7):
        amount = float(input(f"Day {i+1}: ₹"))
        expenses.append(amount)

    total = sum(expenses)
    average = total / len(expenses)
    highest = max(expenses)

    print(f"Total spent: ₹{total}")
    print(f"Average per day: ₹{average:.2f}")
    print(f"Highest spend in a day: ₹{highest}")

# 4. Guess the Number
def guess_the_number():
    secret = random.randint(1, 50)
    attempts = 5

    while attempts > 0:
        guess = int(input("Guess the number (1–50): "))
        if guess == secret:
            print("🎉 Correct! You guessed it!")
            return
        elif guess < secret:
            print("Too Low.")
        else:
            print("Too High.")
        attempts -= 1

    print(f"Game Over. The correct number was: {secret}")

# 5. Student Report Card
def student_report_card():
    name = input("Enter student name: ")
    marks = []
    for i in range(1, 4):
        score = float(input(f"Enter mark for Subject {i}: "))
        marks.append(score)

    total = sum(marks)
    average = total / 3

    if average >= 85:
        grade = "A"
    elif average >= 65:
        grade = "B"
    else:
        grade = "C"

    today = datetime.date.today()
    print("\n--- Report Card ---")
    print(f"Name: {name}")
    print(f"Total Marks: {total}")
    print(f"Average: {average:.2f}")
    print(f"Grade: {grade}")
    print(f"Date: {today}")

# 6. Contact Saver
def contact_saver():
    global contacts
    while True:
        print("\n--- Contact Menu ---")
        print("1. Add Contact")
        print("2. View Contacts")
        print("3. Save & Exit")
        choice = input("Choose an option (1–3): ")

        if choice == '1':
            name = input("Enter name: ")
            phone = input("Enter phone number: ")
            contacts[name] = phone
        elif choice == '2':
            if not contacts:
                print("No contacts saved.")
            else:
                for name, phone in contacts.items():
                    print(f"{name}: {phone}")
        elif choice == '3':
            with open("contacts.txt", "w") as f:
                for name, phone in contacts.items():
                    f.write(f"{name}: {phone}\n")
            print("Contacts saved to contacts.txt")
            break
        else:
            print("Invalid option. Try again.")

# Menu
def main():
    while True:
        print("\n===== Intermediate Python Exercises =====")
        print("1. BMI Calculator")
        print("2. Strong Password Checker")
        print("3. Weekly Expense Calculator")
        print("4. Guess the Number")
        print("5. Student Report Card")
        print("6. Contact Saver")
        print("7. Exit")

        option = input("Choose an option (1–7): ")

        if option == '1':
            bmi_calculator()
        elif option == '2':
            strong_password_checker()
        elif option == '3':
            weekly_expense_calculator()
        elif option == '4':
            guess_the_number()
        elif option == '5':
            student_report_card()
        elif option == '6':
            contact_saver()
        elif option == '7':
            print("Exiting program. Goodbye!")
            break
        else:
            print("Invalid choice. Try again.")

if __name__ == "__main__":
    main()
