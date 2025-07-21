use analytics_practice;
 
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    city VARCHAR(50)
);
 
INSERT INTO customers VALUES
(1, 'Amit Sharma', 'Delhi'),
(2, 'Neha Reddy', 'Hyderabad'),
(3, 'Rahul Iyer', 'Mumbai'),
(4, 'Divya Mehta', 'Chennai');
 
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_name VARCHAR(100),
    order_amount INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
 
INSERT INTO orders VALUES
(101, 1, 'Laptop', 55000),
(102, 2, 'Mouse', 500),
(103, 1, 'Keyboard', 1500),
(104, 3, 'Monitor', 7000),
(105, 2, 'Printer', 8500);

select customers.customer_name, orders.product_name, orders.order_amount from customers inner join orders on customers.customer_id=orders.customer_id;

select customers.customer_name, orders.product_name from customers left join orders on customers.customer_id = orders.customer_id;

select customers.customer_name, orders.product_name from customers right join orders on customers.customer_id = orders.customer_id;

select customers.customer_name, orders.product_name from customers full join orders on customers.customer_id = orders.customer_id;

select customers.customer_name, orders.product_name, orders.order_amount 
from customers
join orders
on customers.customer_id = orders.customer_id
where orders.order_amount > 5000;

select o.order_id, c.customer_name, c.city, o.product_name, o.order_amount
from orders o
join customers c
on o.customer_id=c.customer_id;

select c.customer_name, count(o.order_id) as total_orders
from customers c
join orders o on c.customer_id=o.customer_id
group by c.customer_name
having total_orders > 1;