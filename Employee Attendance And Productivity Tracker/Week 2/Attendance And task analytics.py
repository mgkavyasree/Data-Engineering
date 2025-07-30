import pandas as pd
import numpy as np

# read attendance data
attendance = pd.read_csv('attendance.csv')

# calculate working hours
attendance['clockin'] = pd.to_datetime(attendance['clockin'])
attendance['clockout'] = pd.to_datetime(attendance['clockout'])
attendance['work_hours'] = (attendance['clockout'] - attendance['clockin']).dt.total_seconds() / 3600

# read tasks data
tasks = pd.read_csv('tasks.csv')

# merge both datasets
df = pd.merge(attendance, tasks, on=['employee_id', 'date'], how='left')

# calculate productivity score = tasks_completed / work_hours
df['tasks_completed'] = df['task_status'].apply(lambda x: 1 if x == 'completed' else 0)
df['productivity_score'] = df['tasks_completed'] / df['work_hours']

# group by employee
summary = df.groupby('employee_id')[['work_hours', 'productivity_score']].mean()
print('employee productivity summary:')
print(summary)

# export results
df.to_csv('cleaned_attendance.csv', index=False)
summary.to_csv('summary_by_employee.csv')
