-- create products table
create table products (
  product_id int primary key,
  product_name varchar(100),
  category varchar(50),
  price decimal(10,2),
  cost decimal(10,2)
);

-- create stores table
create table stores (
  store_id int primary key,
  store_name varchar(100),
  region varchar(50)
);

-- create employees table
create table employees (
  employee_id int primary key,
  employee_name varchar(100),
  role varchar(50),
  store_id int,
  foreign key (store_id) references stores(store_id)
);

-- create sales table
create table sales (
  sale_id int primary key,
  product_id int,
  store_id int,
  employee_id int,
  quantity int,
  sale_date date,
  foreign key (product_id) references products(product_id),
  foreign key (store_id) references stores(store_id),
  foreign key (employee_id) references employees(employee_id)
);

-- insert sample data
insert into products values (1, 't-shirt', 'clothing', 20.00, 10.00);
insert into stores values (101, 'store a', 'north');
insert into employees values (1001, 'alice', 'cashier', 101);
insert into sales values (5001, 1, 101, 1001, 5, '2025-07-01');

-- stored procedure to calculate daily sales for a store
delimiter //
create procedure daily_sales_total (
  in input_store_id int,
  in input_date date
)
begin
  select s.store_id, s.sale_date,
         sum(p.price * s.quantity) as total_sales
  from sales s
  join products p on s.product_id = p.product_id
  where s.store_id = input_store_id
    and s.sale_date = input_date
  group by s.store_id, s.sale_date;
end;
//
delimiter ;
