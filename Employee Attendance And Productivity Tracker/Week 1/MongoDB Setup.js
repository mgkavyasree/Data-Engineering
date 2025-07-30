// use hr database
use hr;

db.task_notes.insertMany([
  {
    employee_id: 1,
    note: "delivered high quality sales presentation",
    date: "2025-07-27",
    manager: "raj"
  },
  {
    employee_id: 1,
    note: "missed deadline for weekly report",
    date: "2025-07-25",
    manager: "raj"
  }
]);

// create indexes for faster lookup
db.task_notes.createIndex({ employee_id: 1 });
db.task_notes.createIndex({ department: 1 });
