// use employee_attendence

db.createCollection("taskFeedbacks")

db.createCollection("employeeNotes")

db.taskgeedbacks.insertOne(
  {
employeeID: 1,
taskID: 1,
date: new Date("2024-06-01"),
feedback: "Good work!"
}
)

db.employeeNotes.insertOne(
  {
employeeID: 1,
department: "AI",
noteType: "Warning",
note: "Absent 3 days in last week",
createdBy: "AI_Engineerr_01",
timestamp: new Date("2024-06-01")
}
)

db.employeeNotes.createIndex({employeeID: 1})

db.taskfeebacks.createIndex({taskID: 1})