use dataengineer;

-- create students table
create table students (
  student_id int primary key,
  name varchar(100),
  age int,
  gender varchar(10),
  department_id int
);

-- create departments table
create table departments (
  department_id int primary key,
  department_name varchar(100),
  head_of_department varchar(100)
);

-- create courses table
create table courses (
  course_id int primary key,
  course_name varchar(100),
  department_id int,
  credit_hours int
);

-- insert into students
insert into students values
(1, 'amit sharma', 20, 'male', 1),
(2, 'neha reddy', 22, 'female', 2),
(3, 'faizan ali', 21, 'male', 1),
(4, 'divya mehta', 23, 'female', 3),
(5, 'ravi verma', 22, 'male', 2);

-- insert into departments
insert into departments (department_id, department_name, head_of_department) values
(1, 'computer science', 'dr. rao'),
(2, 'electronics', 'dr. iyer'),
(3, 'mechanical', 'dr. khan');

-- insert into courses
insert into courses (course_id, course_name, department_id, credit_hours) values
(101, 'data structures', 1, 4),
(102, 'circuits', 2, 3),
(103, 'thermodynamics', 3, 4),
(104, 'algorithms', 1, 3),
(105, 'microcontrollers', 2, 2);

-- section (a)
-- list all students with name, age, and gender
select name, age, gender from students;

-- show names of female students only
select name from students
where gender = 'female';

-- display all courses offered by the electronics department
select course_name from courses
where department_id = (
  select department_id from departments
  where department_name = 'electronics'
);

-- show the department name and head for department_id = 1
select department_name, head_of_department
from departments
where department_id = 1;

-- display students older than 21 years
select * from students
where age > 21;

-- section (b) : intermediate joins & aggregations
-- show student names along with their department names
select s.name, d.department_name
from students s
join departments d on s.department_id = d.department_id;

-- list all departments with number of students in each
select d.department_name, count(s.student_id) as student_count
from departments d
left join students s on d.department_id = s.department_id
group by d.department_name;

-- find the average age of students per department
select d.department_name, avg(s.age) as average_age
from departments d
join students s on d.department_id = s.department_id
group by d.department_name;

-- show all courses with their respective department names
select c.course_name, d.department_name
from courses c
join departments d on c.department_id = d.department_id;

-- list departments that have no students enrolled
select d.department_name
from departments d
left join students s on d.department_id = s.department_id
where s.student_id is null;

-- show the department that has the highest number of courses
select d.department_name
from departments d
join courses c on d.department_id = c.department_id
group by d.department_name
order by count(c.course_id) desc
limit 1;

-- section (c) : subqueries & advanced filters
-- find names of students whose age is above the average age of all students
select name from students
where age > (select avg(age) from students);

-- show all departments that offer courses with more than 3 credit hours
select distinct d.department_name
from departments d
join courses c on d.department_id = c.department_id
where c.credit_hours > 3;

-- display names of students who are enrolled in the department with the fewest courses
select name from students
where department_id = (
  select department_id
  from courses
  group by department_id
  order by count(*) asc
  limit 1
);

-- list the names of students in departments where the hod's name contains 'dr.'
select s.name
from students s
join departments d on s.department_id = d.department_id
where d.head_of_department like '%dr.%';

-- find the second oldest student
select * from students
order by age desc
limit 1 offset 1;

select * from students
where age = (
  select max(age)
  from students
  where age < (select max(age) from students)
);

-- show all courses that belong to departments with more than 2 students
select c.course_name
from courses c
where c.department_id in (
  select department_id
  from students
  group by department_id
  having count(*) > 2
);


