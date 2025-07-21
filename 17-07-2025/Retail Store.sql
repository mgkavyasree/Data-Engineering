create database retail_store;
use retail_store;

create table products (
    product_id int primary key,
    product_name varchar(100),
    category varchar(50),
    price decimal(10, 2),
    stock int,
    added_date date
);

insert into products values 
(1, 'smartphone', 'electronics', 1200.00, 15, '2023-03-15'),
(2, 'sofa', 'furniture', 800.00, 5, '2023-05-10'),
(3, 'headphones', 'accessories', 150.00, 25, '2022-12-01'),
(4, 'sneakers', 'footwear', 600.00, 8, '2023-06-20'),
(5, 'smartwatch', 'wearables', 1800.00, 0, '2023-07-01');

select* from products;