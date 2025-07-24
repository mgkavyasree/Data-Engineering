-- Travel Planner

-- create table: destinations
create table destinations (
    destination_id int primary key,
    city varchar(50),
    country varchar(50),
    category varchar(50), 
    avg_cost_per_day decimal(10,2)
);

-- create table: trips
create table trips (
    trip_id int primary key,
    destination_id int,
    traveler_name varchar(50),
    start_date date,
    end_date date,
    budget decimal(10,2),
    foreign key (destination_id) references destinations(destination_id)
);

-- insert sample data into destinations
insert into destinations values
(1, 'goa', 'india', 'beach', 2500),
(2, 'paris', 'france', 'historical', 6000),
(3, 'bali', 'indonesia', 'beach', 2800),
(4, 'rome', 'italy', 'historical', 5500),
(5, 'leh', 'india', 'adventure', 2200),
(6, 'zurich', 'switzerland', 'nature', 7000);

-- insert sample data into trips
insert into trips values
(1, 1, 'ananya', '2025-01-10', '2025-01-15', 15000),
(2, 2, 'rohit', '2024-12-01', '2024-12-08', 50000),
(3, 3, 'meera', '2025-03-05', '2025-03-10', 20000),
(4, 1, 'vijay', '2025-04-01', '2025-04-05', 12000),
(5, 4, 'ananya', '2024-11-01', '2024-11-12', 40000),
(6, 5, 'sachin', '2025-05-01', '2025-05-08', 18000),
(7, 3, 'ananya', '2025-06-01', '2025-06-10', 25000),
(8, 6, 'meera', '2025-07-01', '2025-07-09', 56000),
(9, 1, 'sachin', '2024-09-01', '2024-09-06', 14000),
(10, 5, 'rohit', '2025-01-15', '2025-01-22', 16000);

-- show all trips to destinations in 'india'
select * from trips
join destinations on trips.destination_id = destinations.destination_id
where country = 'india';

-- list all destinations with an average cost below 3000
select * from destinations
where avg_cost_per_day < 3000;

-- calculate the number of days for each trip
select trip_id, traveler_name, datediff(end_date, start_date) + 1 as trip_duration_days
from trips;

-- list all trips that last more than 7 days
select * from trips
where datediff(end_date, start_date) + 1 > 7;

-- list traveler name, destination city, and total trip cost (duration * avg_cost_per_day)
select t.traveler_name, d.city, (datediff(t.end_date, t.start_date) + 1) * d.avg_cost_per_day as total_trip_cost
from trips t
join destinations d on t.destination_id = d.destination_id;

-- find the total number of trips per country
select d.country, count(*) as total_trips
from trips t
join destinations d on t.destination_id = d.destination_id
group by d.country;

-- show average budget per country
select d.country, avg(t.budget) as avg_budget
from trips t
join destinations d on t.destination_id = d.destination_id
group by d.country;

-- find which traveler has taken the most trips
select traveler_name, count(*) as total_trips
from trips
group by traveler_name
order by total_trips desc
limit 1;

-- show destinations that haven’t been visited yet
select * from destinations
where destination_id not in (select distinct destination_id from trips);

-- find the trip with the highest cost per day (budget / duration)
select trip_id, traveler_name, round(budget / (datediff(end_date, start_date) + 1), 2) as cost_per_day
from trips
order by cost_per_day desc
limit 1;

-- update the budget for a trip that was extended by 3 days (trip_id = 1)
update trips
set end_date = date_add(end_date, interval 3 day),
    budget = budget + (3 * (select avg_cost_per_day from destinations where destination_id = trips.destination_id))
where trip_id = 1;

-- delete all trips that were completed before jan 1, 2023
delete from trips
where end_date < '2023-01-01';
