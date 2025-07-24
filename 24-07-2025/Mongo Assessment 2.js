// Library Management System
// books
db.books.insertMany([
  { book_id: 201, title: "the alchemist", author: "paulo coelho", genre: "fiction", copies: 10 },
  { book_id: 202, title: "atomic habits", author: "james clear", genre: "self-help", copies: 5 },
  { book_id: 203, title: "sapiens", author: "yuval noah harari", genre: "history", copies: 7 },
  { book_id: 204, title: "the lean startup", author: "eric ries", genre: "business", copies: 3 },
  { book_id: 205, title: "deep work", author: "cal newport", genre: "productivity", copies: 4 }
])

// members
db.members.insertMany([
  { member_id: 101, name: "ayesha khan", joined_on: new Date("2024-01-15") },
  { member_id: 102, name: "rahul verma", joined_on: new Date("2024-03-12") },
  { member_id: 103, name: "nikita rao", joined_on: new Date("2024-04-10") }
])

// borrowed
db.borrowed.insertMany([
  { borrow_id: 1, member_id: 101, book_id: 201, date: new Date("2024-06-01"), returned: true },
  { borrow_id: 2, member_id: 101, book_id: 203, date: new Date("2024-06-15"), returned: false },
  { borrow_id: 3, member_id: 102, book_id: 202, date: new Date("2024-06-20"), returned: false },
  { borrow_id: 4, member_id: 103, book_id: 204, date: new Date("2024-06-22"), returned: true }
])

/find all books in the self-help genre
db.books.find({ genre: "self-help" })

// show members who joined after march 2024
db.members.find({ joined_on: { $gt: new Date("2024-03-31") } })

// list all borrowed books that have not been returned
db.borrowed.find({ returned: false })

// display all books with fewer than 5 copies in stock
db.books.find({ copies: { $lt: 5 } })

// get details of all books written by cal newport
db.books.find({ author: "cal newport" })

// join queries using $lookup
// list all borrow records with book title and member name
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  { $unwind: "$book_info" },
  { $unwind: "$member_info" },
  {
    $project: {
      borrow_id: 1,
      date: 1,
      returned: 1,
      book_title: "$book_info.title",
      member_name: "$member_info.name"
    }
  }
])

// find which member borrowed "sapiens"
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  { $match: { "book_info.title": "sapiens" } },
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  { $unwind: "$member_info" },
  {
    $project: {
      member_name: "$member_info.name"
    }
  }
])

// display all members along with the books they've borrowed
db.members.aggregate([
  {
    $lookup: {
      from: "borrowed",
      localField: "member_id",
      foreignField: "member_id",
      as: "borrowed_books"
    }
  }
])

// get a list of members who have borrowed books and not returned them
db.borrowed.aggregate([
  { $match: { returned: false } },
  {
    $lookup: {
      from: "members",
      localField: "member_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  { $unwind: "$member_info" },
  { $group: { _id: "$member_info.name" } }
])

// show each book along with how many times it has been borrowed
db.borrowed.aggregate([
  {
    $group: {
      _id: "$book_id",
      borrow_count: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "books",
      localField: "_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $project: {
      book_title: "$book_info.title",
      borrow_count: 1
    }
  }
])

// aggregations & analysis
// count how many books each member has borrowed
db.borrowed.aggregate([
  {
    $group: {
      _id: "$member_id",
      total_borrowed: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "members",
      localField: "_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  { $unwind: "$member_info" },
  {
    $project: {
      member_name: "$member_info.name",
      total_borrowed: 1
    }
  }
])

// which genre has the highest number of books
db.books.aggregate([
  {
    $group: {
      _id: "$genre",
      total: { $sum: "$copies" }
    }
  },
  { $sort: { total: -1 } },
  { $limit: 1 }
])

// list the top 2 most borrowed books
db.borrowed.aggregate([
  {
    $group: {
      _id: "$book_id",
      borrow_count: { $sum: 1 }
    }
  },
  { $sort: { borrow_count: -1 } },
  { $limit: 2 },
  {
    $lookup: {
      from: "books",
      localField: "_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $project: {
      book_title: "$book_info.title",
      borrow_count: 1
    }
  }
])

// show the average number of copies available per genre
db.books.aggregate([
  {
    $group: {
      _id: "$genre",
      avg_copies: { $avg: "$copies" }
    }
  }
])

// find the total number of books currently borrowed (not returned)
db.borrowed.aggregate([
  { $match: { returned: false } },
  { $count: "total_currently_borrowed" }
])

// add a new member who hasn't borrowed any book
db.members.insertOne({ member_id: 104, name: "sanjay menon", joined_on: new Date("2024-07-01") })

// list members who haven't borrowed any book
db.members.aggregate([
  {
    $lookup: {
      from: "borrowed",
      localField: "member_id",
      foreignField: "member_id",
      as: "borrowed_books"
    }
  },
  { $match: { borrowed_books: { $eq: [] } } }
])

// identify books that have never been borrowed
db.books.aggregate([
  {
    $lookup: {
      from: "borrowed",
      localField: "book_id",
      foreignField: "book_id",
      as: "borrow_info"
    }
  },
  { $match: { borrow_info: { $eq: [] } } }
])

// get the name of members who borrowed more than one book
db.borrowed.aggregate([
  {
    $group: {
      _id: "$member_id",
      borrow_count: { $sum: 1 }
    }
  },
  { $match: { borrow_count: { $gt: 1 } } },
  {
    $lookup: {
      from: "members",
      localField: "_id",
      foreignField: "member_id",
      as: "member_info"
    }
  },
  { $unwind: "$member_info" },
  {
    $project: {
      member_name: "$member_info.name",
      borrow_count: 1
    }
  }
])

// display borrowing trends by month
db.borrowed.aggregate([
  {
    $group: {
      _id: { $month: "$date" },
      borrow_count: { $sum: 1 }
    }
  },
  { $sort: { "_id": 1 } }
])

// show borrow records where the borrowed book had fewer than 5 copies at the time
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  { $match: { "book_info.copies": { $lt: 5 } } },
  {
    $project: {
      borrow_id: 1,
      member_id: 1,
      book_title: "$book_info.title",
      copies_at_borrow_time: "$book_info.copies",
      date: 1
    }
  }
])

// simulate overdue books by adding due_date and finding overdue ones
db.borrowed.updateMany({}, { $set: { due_date: new Date("2024-06-30") } }) // simulate due date
db.borrowed.find({ due_date: { $lt: new Date() }, returned: false })

// create a chart-style output showing how many books are borrowed per genre
db.borrowed.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $group: {
      _id: "$book_info.genre",
      borrow_count: { $sum: 1 }
    }
  },
  { $sort: { borrow_count: -1 } }
])
