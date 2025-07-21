create database product;
use product;

-- task 1 
create table products (
      product_id int primary key,
      product_name varchar(30),
      category varchar(30),
      price decimal(10,2),
      stock_quantity int,
      added_date date
);

-- task 2
insert into products values
(1, 'Kurti', 'Clothing', 2000.00, 20, '2025-06-21'),
(2, 'Boat', 'Accessories', 1000.00, 10, '2025-06-22'),
(3, 'Samsung', 'Electronics', 35000.00, 25, '2025-06-23'),
(4, 'Wooden bed', 'Furniture', 40000.00, 9, '2025-06-24'),
(5, 'Necklace', 'Jewellery', 25000.00, 30, '2025-06-25');

-- task 3 
select * from products;

select product_name, price from products;

select * from products
where stock_quantity < 10;

select * from products
where price between 500 and 2000;

select * from products
where added_date > '2023-01-01';

select * from products
where product_name like 'S%';

select * from products
where category in ('Electronics','Furniture');

-- task 4
update products set price = 30000.00 where product_id = 5;

update products set stock_quantity = stock_quantity + 5 where category = 'Electronics';

delete from products where product_id = 1;

delete from products where stock_quantity = 0;
