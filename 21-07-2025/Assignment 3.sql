use dataengineer;

-- part 1
-- create the tables
create table books (
  book_id int primary key,
  title varchar(100),
  author varchar(100),
  genre varchar(50),
  price decimal(10, 2)
);

create table customers (
  customer_id int primary key,
  name varchar(100),
  email varchar(100),
  city varchar(50)
);

create table orders (
  order_id int primary key,
  customer_id int,
  book_id int,
  order_date date,
  quantity int,
  foreign key (customer_id) references customers(customer_id),
  foreign key (book_id) references books(book_id)
);

-- part 2
-- insert sample data
insert into books values
(1, 'the alchemist', 'paulo coelho', 'fiction', 499.99),
(2, 'deep work', 'cal newport', 'self-help', 650.00),
(3, 'atomic habits', 'james clear', 'self-help', 550.00),
(4, 'sapiens', 'yuval noah harari', 'history', 700.00),
(5, 'zero to one', 'peter thiel', 'business', 480.00);

insert into customers values
(1, 'anita reddy', 'anita@example.com', 'hyderabad'),
(2, 'rahul sharma', 'rahul@example.com', 'delhi'),
(3, 'mohit verma', 'mohit@example.com', 'mumbai'),
(4, 'sneha nair', 'sneha@example.com', 'hyderabad'),
(5, 'ajay kumar', 'ajay@example.com', 'chennai');

insert into orders values
(1, 1, 2, '2023-01-05', 1),
(2, 2, 3, '2023-03-15', 2),
(3, 3, 1, '2022-12-20', 1),
(4, 4, 4, '2023-04-10', 1),
(5, 1, 5, '2023-06-01', 1),
(6, 5, 2, '2023-07-07', 1),
(7, 1, 3, '2023-08-18', 1);

-- part 3
-- list all books with price above 500
select * from books where price > 500;

-- show all customers from the city of 'hyderabad'
select * from customers where city = 'hyderabad';

-- find all orders placed after '2023-01-01'
select * from orders where order_date > '2023-01-01';

-- joins & aggregations
-- show customer names along with book titles they purchased
select c.name, b.title
from orders o
join customers c on o.customer_id = c.customer_id
join books b on o.book_id = b.book_id;

-- list each genre and total number of books sold in that genre
select b.genre, sum(o.quantity) as total_sold
from orders o
join books b on o.book_id = b.book_id
group by b.genre;

-- find the total sales amount (price × quantity) for each book
select b.title, sum(b.price * o.quantity) as total_sales
from orders o
join books b on o.book_id = b.book_id
group by b.title;

-- show the customer who placed the highest number of orders
select c.name, count(*) as total_orders
from orders o
join customers c on o.customer_id = c.customer_id
group by c.name
order by total_orders desc
limit 1;

-- display average price of books by genre
select genre, avg(price) as average_price
from books
group by genre;

-- list all books that have not been ordered
select * from books
where book_id not in (select distinct book_id from orders);

-- show the name of the customer who has spent the most in total
select c.name, sum(b.price * o.quantity) as total_spent
from orders o
join customers c on o.customer_id = c.customer_id
join books b on o.book_id = b.book_id
group by c.name
order by total_spent desc
limit 1;
