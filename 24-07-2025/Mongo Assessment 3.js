// Design & Query Challenge

// part 1: create collections
// jobs collection
db.jobs.insertMany([
  { job_id: 1, title: "full stack developer", company: "techwave", location: "bangalore", salary: 1200000, job_type: "remote", posted_on: new Date("2024-06-25") },
  { job_id: 2, title: "data analyst", company: "databricks", location: "hyderabad", salary: 900000, job_type: "hybrid", posted_on: new Date("2024-07-01") },
  { job_id: 3, title: "backend engineer", company: "techwave", location: "delhi", salary: 1500000, job_type: "on-site", posted_on: new Date("2024-07-10") },
  { job_id: 4, title: "devops engineer", company: "cloudzen", location: "mumbai", salary: 1300000, job_type: "remote", posted_on: new Date("2024-07-15") },
  { job_id: 5, title: "ui/ux designer", company: "creativio", location: "chennai", salary: 800000, job_type: "on-site", posted_on: new Date("2024-06-20") }
])

// applicants collection
db.applicants.insertMany([
  { applicant_id: 101, name: "kavya sree", skills: ["mongodb", "node.js", "react"], experience: 2, city: "hyderabad", applied_on: new Date("2024-07-12") },
  { applicant_id: 102, name: "arjun mehta", skills: ["python", "sql", "excel"], experience: 3, city: "pune", applied_on: new Date("2024-07-10") },
  { applicant_id: 103, name: "neha varma", skills: ["java", "spring", "mongodb"], experience: 4, city: "bangalore", applied_on: new Date("2024-07-01") },
  { applicant_id: 104, name: "rahul jain", skills: ["aws", "docker", "linux"], experience: 5, city: "hyderabad", applied_on: new Date("2024-07-08") },
  { applicant_id: 105, name: "sneha iyer", skills: ["figma", "ui design", "css"], experience: 2, city: "chennai", applied_on: new Date("2024-06-28") }
])

// applications collection
db.applications.insertMany([
  { application_id: 201, applicant_id: 101, job_id: 1, application_status: "applied", interview_scheduled: false, feedback: "" },
  { application_id: 202, applicant_id: 103, job_id: 2, application_status: "interview scheduled", interview_scheduled: true, feedback: "pending" },
  { application_id: 203, applicant_id: 101, job_id: 4, application_status: "interview scheduled", interview_scheduled: true, feedback: "good" },
  { application_id: 204, applicant_id: 104, job_id: 3, application_status: "applied", interview_scheduled: false, feedback: "" },
  { application_id: 205, applicant_id: 105, job_id: 5, application_status: "rejected", interview_scheduled: false, feedback: "skills mismatch" }
])

// part 2
// find all remote jobs with a salary greater than 10,00,000
db.jobs.find({ job_type: "remote", salary: { $gt: 1000000 } })

// get all applicants who know mongodb
db.applicants.find({ skills: "mongodb" })

// show the number of jobs posted in the last 30 days
db.jobs.countDocuments({ posted_on: { $gte: new Date(Date.now() - 1000 * 60 * 60 * 24 * 30) } })

// list all job applications that are in ‘interview scheduled’ status
db.applications.find({ application_status: "interview scheduled" })

// find companies that have posted more than 2 jobs
db.jobs.aggregate([
  { $group: { _id: "$company", count: { $sum: 1 } } },
  { $match: { count: { $gt: 2 } } }
])


// part 3: lookup & aggregation
// join applications with jobs to show job title along with the applicant’s name
db.applications.aggregate([
  {
    $lookup: {
      from: "jobs",
      localField: "job_id",
      foreignField: "job_id",
      as: "job_info"
    }
  },
  { $unwind: "$job_info" },
  {
    $lookup: {
      from: "applicants",
      localField: "applicant_id",
      foreignField: "applicant_id",
      as: "applicant_info"
    }
  },
  { $unwind: "$applicant_info" },
  {
    $project: {
      job_title: "$job_info.title",
      applicant_name: "$applicant_info.name",
      application_status: 1
    }
  }
])

// find how many applications each job has received
db.applications.aggregate([
  { $group: { _id: "$job_id", applications_received: { $sum: 1 } } }
])

// list applicants who have applied for more than one job
db.applications.aggregate([
  { $group: { _id: "$applicant_id", total_apps: { $sum: 1 } } },
  { $match: { total_apps: { $gt: 1 } } },
  {
    $lookup: {
      from: "applicants",
      localField: "_id",
      foreignField: "applicant_id",
      as: "applicant_info"
    }
  },
  { $unwind: "$applicant_info" },
  { $project: { name: "$applicant_info.name", total_apps: 1 } }
])

// show the top 3 cities with the most applicants
db.applicants.aggregate([
  { $group: { _id: "$city", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 3 }
])

// get the average salary for each job type
db.jobs.aggregate([
  { $group: { _id: "$job_type", avg_salary: { $avg: "$salary" } } }
])

// part 4: data updates
// update the status of one application to "offer made"
db.applications.updateOne({ application_id: 202 }, { $set: { application_status: "offer made" } })

// delete a job that has not received any applications
db.jobs.find().forEach(function(job) {
  const count = db.applications.countDocuments({ job_id: job.job_id });
  if (count === 0) {
    db.jobs.deleteOne({ job_id: job.job_id });
  }
})

// add a new field 'shortlisted' to all applications and set to false
db.applications.updateMany({}, { $set: { shortlisted: false } })

// increment experience of all applicants from "hyderabad" by 1 year
db.applicants.updateMany({ city: "hyderabad" }, { $inc: { experience: 1 } })

// remove all applicants who haven’t applied to any job
db.applicants.find().forEach(function(applicant) {
  const count = db.applications.countDocuments({ applicant_id: applicant.applicant_id });
  if (count === 0) {
    db.applicants.deleteOne({ applicant_id: applicant.applicant_id });
  }
})
