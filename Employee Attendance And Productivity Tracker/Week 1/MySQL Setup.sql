-- create employees table
create table employees (
  employee_id int primary key,
  employee_name varchar(100),
  department varchar(50),
  role varchar(50)
);

-- create attendance table
create table attendance (
  attendance_id int primary key,
  employee_id int,
  clockin datetime,
  clockout datetime,
  date date,
  foreign key (employee_id) references employees(employee_id)
);

-- create tasks table
create table tasks (
  task_id int primary key,
  employee_id int,
  task_name varchar(100),
  task_status varchar(20),
  task_date date,
  foreign key (employee_id) references employees(employee_id)
);

-- insert sample data
insert into employees values (1, 'arjun', 'sales', 'executive');
insert into attendance values (101, 1, '2025-07-27 09:00:00', '2025-07-27 17:00:00', '2025-07-27');
insert into tasks values (1001, 1, 'prepare report', 'completed', '2025-07-27');

-- stored procedure to calculate total hours worked per employee
delimiter //
create procedure total_work_hours (
  in emp_id int
)
begin
  select employee_id,
         sum(timestampdiff(hour, clockin, clockout)) as total_hours
  from attendance
  where employee_id = emp_id
  group by employee_id;
end;
//
delimiter ;
