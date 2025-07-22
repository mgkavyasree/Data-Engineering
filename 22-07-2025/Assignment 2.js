// Movie Streaming App

// Insert Users

db.users.insertMany([
  { user_id: 1, name: "Aarav Mehta", email: "aarav@example.com", country: "India" },
  { user_id: 2, name: "Liam Smith", email: "liam@example.com", country: "USA" },
  { user_id: 3, name: "Emily Zhang", email: "emily@example.com", country: "China" },
  { user_id: 4, name: "Carlos Ruiz", email: "carlos@example.com", country: "Mexico" },
  { user_id: 5, name: "Sara Kim", email: "sara@example.com", country: "South Korea" }
]);

// Insert Movies

db.movies.insertMany([
  { movie_id: 201, title: "Dream Beyond Code", genre: "Sci-Fi", release_year: 2022, duration: 120 },
  { movie_id: 202, title: "Love in Paris", genre: "Romance", release_year: 2019, duration: 105 },
  { movie_id: 203, title: "The Last Warrior", genre: "Action", release_year: 2021, duration: 130 },
  { movie_id: 204, title: "Laugh Out Loud", genre: "Comedy", release_year: 2020, duration: 95 },
  { movie_id: 205, title: "History Rewritten", genre: "Drama", release_year: 2023, duration: 110 },
  { movie_id: 206, title: "Deep Ocean", genre: "Documentary", release_year: 2018, duration: 90 }
]);

// Insert Watch History

db.watch_history.insertMany([
  { watch_id: 1, user_id: 1, movie_id: 201, watched_on: new Date("2023-08-01"), watch_time: 110 },
  { watch_id: 2, user_id: 1, movie_id: 202, watched_on: new Date("2023-08-02"), watch_time: 100 },
  { watch_id: 3, user_id: 2, movie_id: 201, watched_on: new Date("2023-08-02"), watch_time: 120 },
  { watch_id: 4, user_id: 2, movie_id: 203, watched_on: new Date("2023-08-03"), watch_time: 125 },
  { watch_id: 5, user_id: 3, movie_id: 201, watched_on: new Date("2023-08-03"), watch_time: 115 },
  { watch_id: 6, user_id: 3, movie_id: 204, watched_on: new Date("2023-08-03"), watch_time: 95 },
  { watch_id: 7, user_id: 4, movie_id: 202, watched_on: new Date("2023-08-04"), watch_time: 105 },
  { watch_id: 8, user_id: 1, movie_id: 201, watched_on: new Date("2023-08-05"), watch_time: 90 }
]);

// Find all movies with duration > 100 minutes

db.movies.find({ duration: { $gt: 100 } });

// List users from 'India'

db.users.find({ country: "India" });

// Get all movies released after 2020

db.movies.find({ release_year: { $gt: 2020 } });

// Show full watch history: user name, movie title, watch time

db.watch_history.aggregate([
  {
    $lookup: {
      from: "users",
      localField: "user_id",
      foreignField: "user_id",
      as: "user_info"
    }
  },
  { $unwind: "$user_info" },
  {
    $lookup: {
      from: "movies",
      localField: "movie_id",
      foreignField: "movie_id",
      as: "movie_info"
    }
  },
  { $unwind: "$movie_info" },
  {
    $project: {
      _id: 0,
      user_name: "$user_info.name",
      movie_title: "$movie_info.title",
      watch_time: 1
    }
  }
]);

// List each genre and number of times movies in that genre were watched

db.watch_history.aggregate([
  {
    $lookup: {
      from: "movies",
      localField: "movie_id",
      foreignField: "movie_id",
      as: "movie_info"
    }
  },
  { $unwind: "$movie_info" },
  {
    $group: {
      _id: "$movie_info.genre",
      watch_count: { $sum: 1 }
    }
  }
]);

// Display total watch time per user

db.watch_history.aggregate([
  {
    $group: {
      _id: "$user_id",
      total_watch_time: { $sum: "$watch_time" }
    }
  },
  {
    $lookup: {
      from: "users",
      localField: "_id",
      foreignField: "user_id",
      as: "user_info"
    }
  },
  { $unwind: "$user_info" },
  {
    $project: {
      user_name: "$user_info.name",
      total_watch_time: 1
    }
  }
]);

// Find which movie has been watched the most (by count)

db.watch_history.aggregate([
  {
    $group: {
      _id: "$movie_id",
      watch_count: { $sum: 1 }
    }
  },
  { $sort: { watch_count: -1 } },
  { $limit: 1 },
  {
    $lookup: {
      from: "movies",
      localField: "_id",
      foreignField: "movie_id",
      as: "movie_info"
    }
  },
  { $unwind: "$movie_info" },
  {
    $project: {
      movie_title: "$movie_info.title",
      watch_count: 1
    }
  }
]);

// Identify users who have watched more than 2 movies

db.watch_history.aggregate([
  { $group: { _id: "$user_id", movie_ids: { $addToSet: "$movie_id" } } },
  { $project: { movie_count: { $size: "$movie_ids" } } },
  { $match: { movie_count: { $gt: 2 } } }
]);

// 9. Show users who watched the same movie more than once
db.watch_history.aggregate([
  {
    $group: {
      _id: { user_id: "$user_id", movie_id: "$movie_id" },
      watch_times: { $sum: 1 }
    }
  },
  { $match: { watch_times: { $gt: 1 } } },
  {
    $lookup: {
      from: "users",
      localField: "_id.user_id",
      foreignField: "user_id",
      as: "user_info"
    }
  },
  { $unwind: "$user_info" },
  {
    $lookup: {
      from: "movies",
      localField: "_id.movie_id",
      foreignField: "movie_id",
      as: "movie_info"
    }
  },
  { $unwind: "$movie_info" },
  {
    $project: {
      user_name: "$user_info.name",
      movie_title: "$movie_info.title",
      watch_times: 1
    }
  }
]);

// Calculate percentage of each movie watched compared to full duration

db.watch_history.aggregate([
  {
    $lookup: {
      from: "movies",
      localField: "movie_id",
      foreignField: "movie_id",
      as: "movie_info"
    }
  },
  { $unwind: "$movie_info" },
  {
    $project: {
      user_id: 1,
      movie_id: 1,
      watch_time: 1,
      duration: "$movie_info.duration",
      percentage_watched: {
        $multiply: [{ $divide: ["$watch_time", "$movie_info.duration"] }, 100]
      }
    }
  }
]);
