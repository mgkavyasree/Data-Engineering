CREATE DATABASE simple_sql;
USE simple_sql;
 
CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(100),
    department VARCHAR(50),
    salary INT,
    age INT
);
 
INSERT INTO employees VALUES
(1, 'Amit', 'HR', 30000, 25),
(2, 'Neha', 'IT', 45000, 28),
(3, 'Rahul', 'IT', 50000, 30),
(4, 'Divya', 'Sales', 40000, 26),
(5, 'Kiran', 'Sales', 35000, 24),
(6, 'Meena', 'HR', 32000, 29);

select* from employees
where salary > (select avg(salary) from employees);

select dept_avg.department, dept_avg.avg_salary
from (select department, avg(salary) as avg_salary from employees group by department) as dept_avg;

select emp_name, department, salary, rank() over (order by salary desc) as salary_rank from employees;










