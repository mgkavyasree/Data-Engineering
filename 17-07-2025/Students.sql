create database dataengineer; -- create db
use dataengineer; -- use db

-- create table
create table  students (
student_id int primary key,
name varchar(100),
course varchar(100),
join_date date );

-- insert values
insert into students values
(1,'Amit Sharma','Data Engineering','2025-09-15'),
(2,'Neha Verma','Data Science','2025-09-16'),
(3,'Rohit Iyer','Data Engineering','2025-09-17');

-- show table data 
select* from students;
-- show data using where clause 
select name, course from students;
select* from students where course='Data Engineering'; 
select* from students where join_date > '2025-09-15';

-- show data in different variations (and, in, between)
select* from students
where course='Data Engineering' and join_date='2025-09-15';

select* from students
where course in ('Data Science', 'AI');

select* from students
where join_date between '2025-09-15' and '2025-09-20';

-- variations with % 
select* from students where name like 'A%';
select* from students where name like '%a';
select* from students where name like '%it%';

-- update
update students
set course='Advanced Data Engineering'
where student_id=1;

update students
set join_date='2025-09-18'
where name='Neha Verma';

-- delete 
delete from students
where student_id=2;

delete from students
where join_date < '2025-09-16';

-- task 1: create a table
create table products (
    product_id int primary key,
    product_name varchar(100),
    category varchar(50),
    price decimal(10, 2),
    stock_quantity int,
    added_date date
);

-- task 2: insert records
insert into products values (1, 'smartphone', 'electronics', 1200.00, 15, '2023-03-15');
insert into products values (2, 'sofa', 'furniture', 800.00, 5, '2023-05-10');
insert into products values (3, 'headphones', 'accessories', 150.00, 25, '2022-12-01');
insert into products values (4, 'sneakers', 'footwear', 600.00, 8, '2023-06-20');
insert into products values (5, 'smartwatch', 'wearables', 1800.00, 0, '2023-07-01');

-- task 3
-- list all products
select * from products;

-- display only product_name and price
select product_name, price from products;

-- find products with stock_quantity less than 10
select * from products where stock_quantity < 10;

-- find products with price between 500 and 2000
select * from products where price between 500 and 2000;

-- show products added after 2023-01-01
select * from products where added_date > '2023-01-01';


-- list all products whose names start with ‘s’
select * from products where product_name like 's%';

-- show all products that belong to either electronics or furniture
select * from products where category in ('electronics', 'furniture');

-- task 4: update & delete
-- update the price of one product
update products set price = 1300 where product_id = 1;

-- increase stock of all products in a specific category by 5
update products set stock_quantity = stock_quantity + 5 where category = 'electronics';

-- delete one product based on its product_id
delete from products where product_id = 3;

-- delete all products with stock_quantity = 0
delete from products where stock_quantity = 0;

