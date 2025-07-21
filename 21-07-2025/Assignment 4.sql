create database movierentalsystem;
use movierentalsystem;

-- section 1 database design and table creation
create table movies (
    movie_id int primary key,
    title varchar(100),
    genre varchar(50),
    release_year int,
    rental_rate decimal(5,2)
);

create table customers (
    customer_id int primary key,
    name varchar(100),
    email varchar(100),
    city varchar(50)
);

create table rentals (
    rental_id int primary key,
    customer_id int,
    movie_id int,
    rental_date date,
    return_date date,
    foreign key (customer_id) references customers(customer_id),
    foreign key (movie_id) references movies(movie_id)
);

-- section 2 data insertion
-- insert into movies
insert into movies values 
(1, 'inception', 'sci-fi', 2010, 150.00),
(2, 'dil chahta hai', 'drama', 2001, 100.00),
(3, 'tenet', 'action', 2020, 200.00),
(4, 'interstellar', 'sci-fi', 2014, 180.00),
(5, 'rrr', 'action', 2022, 250.00);

-- insert into customers
insert into customers values 
(1, 'amit sharma', 'amit@gmail.com', 'delhi'),
(2, 'neha reddy', 'neha@gmail.com', 'hyderabad'),
(3, 'john doe', 'john@gmail.com', 'bangalore'),
(4, 'divya mehta', 'divya@gmail.com', 'mumbai'),
(5, 'faizan ali', 'faizan@gmail.com', 'bangalore');

-- insert into rentals
insert into rentals values 
(1, 1, 1, '2023-07-01', '2023-07-03'),
(2, 2, 3, '2023-07-02', '2023-07-04'),
(3, 3, 2, '2023-07-02', '2023-07-05'),
(4, 4, 4, '2023-07-03', '2023-07-06'),
(5, 5, 1, '2023-07-04', null),
(6, 1, 3, '2023-07-05', '2023-07-06'),
(7, 2, 5, '2023-07-05', null),
(8, 3, 5, '2023-07-06', '2023-07-07');

-- section 3 
-- movies rented by 'amit sharma'
select m.*
from movies m
join rentals r on m.movie_id = r.movie_id
join customers c on r.customer_id = c.customer_id
where c.name = 'amit sharma';

-- customers from 'bangalore'
select * from customers
where city = 'bangalore';

-- movies released after 2020
select * from movies
where release_year > 2020;

-- aggregate queries
-- number of movies each customer has rented
select c.name, count(r.rental_id) as total_rentals
from customers c
left join rentals r on c.customer_id = r.customer_id
group by c.customer_id;

-- most rented movie title
select m.title, count(r.rental_id) as rental_count
from movies m
join rentals r on m.movie_id = r.movie_id
group by m.movie_id
order by rental_count desc
limit 1;

-- total revenue from all rentals
select sum(m.rental_rate) as total_revenue
from movies m
join rentals r on m.movie_id = r.movie_id;

-- customers who never rented a movie
select c.*
from customers c
left join rentals r on c.customer_id = r.customer_id
where r.rental_id is null;

-- total revenue by genre
select m.genre, sum(m.rental_rate) as revenue
from movies m
join rentals r on m.movie_id = r.movie_id
group by m.genre;

-- customer who spent the most
select c.name, sum(m.rental_rate) as total_spent
from customers c
join rentals r on c.customer_id = r.customer_id
join movies m on r.movie_id = m.movie_id
group by c.customer_id
order by total_spent desc
limit 1;

-- movies rented and not yet returned
select m.title
from movies m
join rentals r on m.movie_id = r.movie_id
where r.return_date is null;

