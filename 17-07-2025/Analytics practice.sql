CREATE DATABASE analytics_practice;
USE analytics_practice;
 
CREATE TABLE sales_data (
    sale_id INT PRIMARY KEY,
    employee_name VARCHAR(100),
    region VARCHAR(50),
    sale_amount DECIMAL(10,2),
    sale_date DATE
);
 
INSERT INTO sales_data VALUES
(1, 'Amit Sharma', 'North', 12000.50, '2024-01-15'),
(2, 'Neha Reddy', 'East', 8500.00, '2024-01-16'),
(3, 'Faizan Ali', 'North', 10000.00, '2024-01-20'),
(4, 'Divya Iyer', 'South', 13000.00, '2024-01-21'),
(5, 'Kiran Mehta', 'East', 9000.00, '2024-01-22'),
(6, 'Amit Sharma', 'North', 15000.00, '2024-02-05'),
(7, 'Neha Reddy', 'East', 8000.00, '2024-02-10'),
(8, 'Faizan Ali', 'North', 7000.00, '2024-02-15'),
(9, 'Divya Iyer', 'South', 14000.00, '2024-02-18'),
(10, 'Kiran Mehta', 'East', 6500.00, '2024-02-20');
 
select employee_name
from sales_data
group by employee_name
having sum(sale_amount) > (select avg(sale_amount) from sales_data);
 