def fizz_buzz():
    for i in range(1, 51):
        if i % 3 == 0 and i % 5 == 0:
            print("FizzBuzz")
        elif i % 3 == 0:
            print("Fizz")
        elif i % 5 == 0:
            print("Buzz")
        else:
            print(i)

def login_simulation():
    correct_username = "admin"
    correct_password = "1234"
    attempts = 3
    while attempts > 0:
        username = input("Enter username: ")
        password = input("Enter password: ")
        if username == correct_username and password == correct_password:
            print("Login Successful!")
            return
        else:
            attempts -= 1
            print(f"Incorrect! Attempts left: {attempts}")
    print("Account Locked")

def palindrome_checker():
    word = input("Enter a word: ")
    if word == word[::-1]:
        print("Palindrome")
    else:
        print("Not a Palindrome")

def prime_in_range():
    n = int(input("Enter a number: "))
    print("Prime numbers from 1 to", n, "are:")
    for num in range(2, n+1):
        for i in range(2, int(num**0.5) + 1):
            if num % i == 0:
                break
        else:
            print(num, end=' ')
    print()

def star_pyramid():
    n = int(input("Enter number of rows: "))
    for i in range(1, n + 1):
        print("*" * i)

def sum_of_digits():
    num = input("Enter a number: ")
    digit_sum = sum(int(d) for d in num)
    print("Sum of digits:", digit_sum)

def multiplication_table():
    num = int(input("Enter a number: "))
    for i in range(1, 11):
        print(f"{num} x {i} = {num * i}")

def count_vowels():
    string = input("Enter a string: ")
    vowels = 'aeiouAEIOU'
    count = sum(1 for ch in string if ch in vowels)
    print("Number of vowels:", count)

def main():
    while True:
        print("\n------ Python Mini Projects Menu ------")
        print("1. FizzBuzz Challenge")
        print("2. Login Simulation (Max 3 Attempts)")
        print("3. Palindrome Checker")
        print("4. Prime Numbers in a Range")
        print("5. Star Pyramid")
        print("6. Sum of Digits")
        print("7. Multiplication Table Generator")
        print("8. Count Vowels in a String")
        print("9. Exit")
        
        choice = input("Choose an option (1-9): ")
        
        if choice == '1':
            fizz_buzz()
        elif choice == '2':
            login_simulation()
        elif choice == '3':
            palindrome_checker()
        elif choice == '4':
            prime_in_range()
        elif choice == '5':
            star_pyramid()
        elif choice == '6':
            sum_of_digits()
        elif choice == '7':
            multiplication_table()
        elif choice == '8':
            count_vowels()
        elif choice == '9':
            print("Exiting program. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
