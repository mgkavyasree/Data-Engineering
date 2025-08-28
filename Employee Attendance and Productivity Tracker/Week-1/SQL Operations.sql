CREATE DATABASE employee_attendence

USE employee_attendence

CREATE TABLE employees(
	employeeID INT IDENTITY(1, 1) PRIMARY KEY,
	name VARCHAR(50),
	department VARCHAR(50),
	role VARCHAR(40),
	email VARCHAR(100),
	hireDate DATETIME DEFAULT GETDATE(),
	status VARCHAR(20) CHECK(status = 'Active' OR status = 'Resigned')
)

CREATE TABLE attendance(
	attendenceID INT IDENTITY(1, 1) PRIMARY KEY,
	employeeID INT,
	date DATE,
	clockIN DATETIME,
	clockOUT DATETIME,
	isLate BIT,
	isAbscent BIT,
	FOREIGN KEY (employeeID) REFERENCES employees(employeeID)
)

CREATE TABLE tasks(
	taskID INT IDENTITY(1, 1) PRIMARY KEY,
	employeeID INT,
	taskName VARCHAR(50),
	taskDate DATE,
	tasksCompeleted INT,
	hoursSpent DECIMAL(5, 2),
	productivityScore DECIMAL(5, 2),
	FOREIGN KEY (employeeID) REFERENCES employees(employeeID)
)

INSERT INTO employees
VALUES
	('John Doe', 'Engineering', 'Software Developer', 'john.doe@example.com', '2023-01-15', 'Active'),
	('Jane Smith', 'Marketing', 'Content Strategist', 'jane.smith@example.com', '2022-11-20', 'Active'),
	('Alice Johnson', 'HR', 'HR Manager', 'alice.johnson@example.com', '2021-09-10', 'Active'),
	('Bob Brown', 'Engineering', 'DevOps Engineer', 'bob.brown@example.com', '2023-05-01', 'Active'),
	('Eva Green', 'Finance', 'Accountant', 'eva.green@example.com', '2022-06-30', 'Resigned')

INSERT INTO attendance
VALUES
	(1, '2024-06-01', '2024-06-01 09:02:00', '2024-06-01 17:00:00', 1, 0),
	(2, '2024-06-01', '2024-06-01 08:55:00', '2024-06-01 17:10:00', 0, 0),
	(3, '2024-06-01', NULL, NULL, 0, 1),
	(4, '2024-06-01', '2024-06-01 09:10:00', '2024-06-01 17:05:00', 1, 0),
	(5, '2024-06-01', '2024-06-01 08:50:00', '2024-06-01 16:45:00', 0, 0)


INSERT INTO tasks
VALUES
	(1, 'API Integration', '2024-06-01', 5, 6.0, 0.83),
	(2, 'Content Calendar Creation', '2024-06-01', 3, 4.5, 0.67),
	(3, 'Policy Review', '2024-06-01', 0, 0.0, 0.0),
	(4, 'CI/CD Setup', '2024-06-01', 4, 5.0, 0.80),
	(5, 'Invoice Auditing', '2024-06-01', 6, 7.5, 0.80)

CREATE INDEX employee_id ON employees(employeeID)

CREATE PROCEDURE workHrs 
AS 
BEGIN
	SELECT e1.name, SUM(DATEDIFF(MINUTE, e2.clockIN, e2.clockOUT)) / 60 AS totalHours FROM employees e1
	INNER JOIN attendance e2 ON e1.employeeID = e2.employeeID
	WHERE e2.clockIN IS NOT NULL AND e2.clockOUT IS NOT NULL
	GROUP BY e1.name
	ORDER BY totalHours DESC
END

EXEC workHrs

SELECT * FROM employees
SELECT * FROM attendance
SELECT * FROM tasks