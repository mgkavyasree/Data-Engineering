use dataengineer;

-- create employees table
create table employees (
  emp_id int primary key,
  emp_name varchar(100),
  department varchar(50),
  salary int,
  age int
);

-- create departments table
create table departments (
  dept_id int primary key,
  dept_name varchar(50),
  location varchar(50)
);

--  insert data into employees
insert into employees values
(101, 'amit sharma', 'engineering', 60000, 30),
(102, 'neha reddy', 'marketing', 45000, 28),
(103, 'faizan ali', 'engineering', 58000, 32),
(104, 'divya mehta', 'hr', 40000, 29),
(105, 'ravi verma', 'sales', 35000, 26);

-- insert data into departments
insert into departments values
(1, 'engineering', 'bangalore'),
(2, 'marketing', 'mumbai'),
(3, 'hr', 'delhi'),
(4, 'sales', 'chennai');

--  section (a)
-- display all employees
select * from employees;

-- show only emp_name and salary
select emp_name, salary from employees;

-- find employees with salary > 40000
select * from employees
where salary > 40000;

-- list employees between age 28 and 32
select * from employees
where age between 28 and 32;

-- show employees not in hr department
select * from employees
where department != 'hr';

-- sort employees by salary descending
select * from employees
order by salary desc;

-- count number of employees
select count(*) as employee_count from employees;

-- find employee with highest salary
select * from employees
order by salary desc
limit 1;

select * from employees
where salary = (select max(salary) from employees);

-- section (b) : joins & aggregations
-- employee names with department location
select e.emp_name, d.location
from employees e
join departments d on e.department = d.dept_name;

-- departments and count of employees
select d.dept_name, count(e.emp_id) as employee_count
from departments d
left join employees e on d.dept_name = e.department
group by d.dept_name;

-- average salary per department
select department, avg(salary) as average_salary
from employees
group by department;

-- departments with no employees
select d.dept_name
from departments d
left join employees e on d.dept_name = e.department
where e.emp_id is null;

-- total salary by department
select department, sum(salary) as total_salary
from employees
group by department;

-- departments with avg salary > 45000
select department, avg(salary) as avg_salary
from employees
group by department
having avg(salary) > 45000;

-- employee name and department earning > 50000
select emp_name, department
from employees
where salary > 50000;
