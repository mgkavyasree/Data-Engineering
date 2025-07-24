-- Personal Fitness Tracker

-- create tables
create table exercises (
    exercise_id int primary key,
    exercise_name varchar(50),
    category varchar(30),
    calories_burn_per_min int
);

create table workoutlog (
    log_id int primary key,
    exercise_id int,
    date date,
    duration_min int,
    mood varchar(20),
    foreign key (exercise_id) references exercises(exercise_id)
);

-- insert sample data into exercises
insert into exercises values
(1, 'running', 'cardio', 10),
(2, 'cycling', 'cardio', 8),
(3, 'push-ups', 'strength', 7),
(4, 'yoga', 'flexibility', 5),
(5, 'plank', 'strength', 6);

-- insert workout logs (2 logs per exercise, different dates)
insert into workoutlog values
(1, 1, '2025-03-01', 30, 'energized'),
(2, 1, '2025-03-05', 25, 'tired'),
(3, 2, '2025-03-02', 40, 'normal'),
(4, 2, '2025-04-01', 35, 'energized'),
(5, 3, '2025-03-03', 20, 'normal'),
(6, 3, '2025-03-10', 30, 'tired'),
(7, 4, '2025-03-04', 60, 'normal'),
(8, 4, '2025-03-12', 45, 'energized'),
(9, 5, '2025-02-25', 15, 'tired'),
(10, 5, '2025-03-15', 20, 'normal');

-- show all exercises under the “cardio” category
select * from exercises
where category = 'cardio';

-- show workouts done in the month of march 2025
select * from workoutlog
where month(date) = 3 and year(date) = 2025;

-- calculate total calories burned per workout (duration × calories_burn_per_min)
select 
    w.log_id,
    e.exercise_name,
    w.duration_min,
    e.calories_burn_per_min,
    (w.duration_min * e.calories_burn_per_min) as total_calories_burned
from workoutlog w
join exercises e on w.exercise_id = e.exercise_id;

-- calculate average workout duration per category
select 
    e.category,
    avg(w.duration_min) as avg_duration
from workoutlog w
join exercises e on w.exercise_id = e.exercise_id
group by e.category;

-- list exercise name, date, duration, and calories burned using a join
select 
    e.exercise_name,
    w.date,
    w.duration_min,
    (w.duration_min * e.calories_burn_per_min) as calories_burned
from workoutlog w
join exercises e on w.exercise_id = e.exercise_id;

-- show total calories burned per day
select 
    w.date,
    sum(w.duration_min * e.calories_burn_per_min) as total_calories
from workoutlog w
join exercises e on w.exercise_id = e.exercise_id
group by w.date;

-- find the exercise that burned the most calories in total
select 
    e.exercise_name,
    sum(w.duration_min * e.calories_burn_per_min) as total_calories
from workoutlog w
join exercises e on w.exercise_id = e.exercise_id
group by e.exercise_name
order by total_calories desc
limit 1;

-- list exercises never logged in the workout log
select * from exercises
where exercise_id not in (select distinct exercise_id from workoutlog);

-- show workouts where mood was “tired” and duration > 30 mins
select * from workoutlog
where mood = 'tired' and duration_min > 30;

-- update a workout log to correct a wrongly entered mood
update workoutlog
set mood = 'normal'
where log_id = 2;

-- update the calories per minute for “running”
update exercises
set calories_burn_per_min = 12
where exercise_name = 'running';

-- delete all logs from february 2024
delete from workoutlog
where month(date) = 2 and year(date) = 2024;
