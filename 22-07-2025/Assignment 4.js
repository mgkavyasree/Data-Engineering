// Fitness Center Database
use fitnesscenterDB;

// Step 1: Create Collections and Insert Data

// Members Collection

db.members.insertMany([
  { member_id: 1, name: "Anjali Rao", age: 28, gender: "female", city: "mumbai", membership_type: "gold" },
  { member_id: 2, name: "Rohan Mehta", age: 35, gender: "male", city: "delhi", membership_type: "silver" },
  { member_id: 3, name: "Fatima Shaikh", age: 22, gender: "female", city: "hyderabad", membership_type: "platinum" },
  { member_id: 4, name: "Vikram Das", age: 41, gender: "male", city: "bangalore", membership_type: "gold" },
  { member_id: 5, name: "Neha Kapoor", age: 31, gender: "female", city: "pune", membership_type: "silver" }
])

// Trainers Collection

db.trainers.insertMany([
  { trainer_id: 101, name: "Ajay Kumar", specialty: "weight training", experience: 7 },
  { trainer_id: 102, name: "Swati Nair", specialty: "cardio", experience: 5 },
  { trainer_id: 103, name: "Imran Qureshi", specialty: "yoga", experience: 8 }
])

// Sessions Collection

db.sessions.insertMany([
  { session_id: 201, member_id: 1, trainer_id: 101, session_type: "strength", duration: 60, date: new Date("2024-08-01") },
  { session_id: 202, member_id: 2, trainer_id: 102, session_type: "cardio", duration: 45, date: new Date("2024-08-02") },
  { session_id: 203, member_id: 3, trainer_id: 103, session_type: "yoga", duration: 90, date: new Date("2024-08-03") },
  { session_id: 204, member_id: 1, trainer_id: 102, session_type: "cardio", duration: 30, date: new Date("2024-08-04") },
  { session_id: 205, member_id: 4, trainer_id: 101, session_type: "strength", duration: 75, date: new Date("2024-08-05") },
  { session_id: 206, member_id: 5, trainer_id: 103, session_type: "yoga", duration: 60, date: new Date("2024-08-05") }
])

// Step 2

// Find all members from mumbai

db.members.find({ city: "mumbai" })

// List all trainers with experience greater than 6 years

db.trainers.find({ experience: { $gt: 6 } })

// Get all yoga sessions

db.sessions.find({ session_type: "yoga" })

// Show all sessions conducted by trainer swati nair

const swati = db.trainers.findOne({ name: "Swati Nair" }).trainer_id
db.sessions.find({ trainer_id: swati })

// Find all members who attended a session on 2024-08-05

const date = new Date("2024-08-05")
db.sessions.find({ date: date })

// Count the number of sessions each member has attended

db.sessions.aggregate([
  { $group: { _id: "$member_id", session_count: { $sum: 1 } } }
])

// Show average duration of sessions for each session_type

db.sessions.aggregate([
  { $group: { _id: "$session_type", avg_duration: { $avg: "$duration" } } }
])

// Find all female members who attended a session longer than 60 minutes

db.sessions.aggregate([
  { $match: { duration: { $gt: 60 } } },
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member"
    }
  },
  { $unwind: "$member" },
  { $match: { "member.gender": "female" } },
  { $project: { member_name: "$member.name", duration: 1 } }
])

// Display sessions sorted by duration (descending)

db.sessions.find().sort({ duration: -1 })

// Find members who have attended sessions with more than one trainer

db.sessions.aggregate([
  {
    $group: {
      _id: "$member_id",
      unique_trainers: { $addToSet: "$trainer_id" }
    }
  },
  {
    $match: {
      "unique_trainers.1": { $exists: true }
    }
  }
])

// Display sessions with member name and trainer name using $lookup

db.sessions.aggregate([
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member"
    }
  },
  { $unwind: "$member" },
  {
    $lookup: {
      from: "trainers",
      localField: "trainer_id",
      foreignField: "trainer_id",
      as: "trainer"
    }
  },
  { $unwind: "$trainer" },
  {
    $project: {
      session_type: 1,
      duration: 1,
      date: 1,
      member_name: "$member.name",
      trainer_name: "$trainer.name"
    }
  }
])

// Calculate total session time per trainer

db.sessions.aggregate([
  { $group: { _id: "$trainer_id", total_duration: { $sum: "$duration" } } }
])

// List each member and their total time spent in the gym

db.sessions.aggregate([
  { $group: { _id: "$member_id", total_duration: { $sum: "$duration" } } }
])

// Count how many sessions each trainer has conducted

db.sessions.aggregate([
  { $group: { _id: "$trainer_id", session_count: { $sum: 1 } } }
])

// Trainer with longest average session duration

db.sessions.aggregate([
  { $group: { _id: "$trainer_id", avg_duration: { $avg: "$duration" } } },
  { $sort: { avg_duration: -1 } },
  { $limit: 1 }
])

// How many unique members each trainer has trained

db.sessions.aggregate([
  { $group: { _id: "$trainer_id", unique_members: { $addToSet: "$member_id" } } },
  { $project: { unique_member_count: { $size: "$unique_members" } } }
])

// Most active member by total session duration

db.sessions.aggregate([
  { $group: { _id: "$member_id", total_duration: { $sum: "$duration" } } },
  { $sort: { total_duration: -1 } },
  { $limit: 1 }
])

// Gold members who took at least one strength session

db.sessions.aggregate([
  { $match: { session_type: "strength" } },
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member"
    }
  },
  { $unwind: "$member" },
  { $match: { "member.membership_type": "gold" } },
  { $project: { member_name: "$member.name", session_type: 1 } }
])

// Breakdown of sessions by membership type

db.sessions.aggregate([
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member"
    }
  },
  { $unwind: "$member" },
  { $group: { _id: "$member.membership_type", session_count: { $sum: 1 } } }
])

// Find members who haven't attended any session

db.members.aggregate([
  {
    $lookup: {
      from: "sessions",
      localField: "member_id",
      foreignField: "member_id",
      as: "sessions"
    }
  },
  { $match: { sessions: { $size: 0 } } }
])
