// switch to bookstore database
use bookstoredb;

// insert sample data into books collection-

db.books.insertmany([
  {
    book_id: 101,
    title: "the ai revolution",
    author: "ray kurzweil",
    genre: "technology",
    price: 799,
    stock: 20
  },
  {
    book_id: 102,
    title: "secrets of the mind",
    author: "v.s. ramachandran",
    genre: "psychology",
    price: 499,
    stock: 15
  },
  {
    book_id: 103,
    title: "midnight tales",
    author: "r.k. narayan",
    genre: "fiction",
    price: 399,
    stock: 25
  },
  {
    book_id: 104,
    title: "data structures unleashed",
    author: "robert lafore",
    genre: "computer science",
    price: 950,
    stock: 10
  },
  {
    book_id: 105,
    title: "spiritual india",
    author: "sadhguru",
    genre: "spirituality",
    price: 350,
    stock: 30
  }
])

// insert sample data into customers collection

db.customers.insertmany([
  { customer_id: 201, name: "amit sharma", email: "amit@example.com", city: "delhi" },
  { customer_id: 202, name: "neha rao", email: "neha@example.com", city: "hyderabad" },
  { customer_id: 203, name: "kiran patel", email: "kiran@example.com", city: "ahmedabad" },
  { customer_id: 204, name: "sana khan", email: "sana@example.com", city: "hyderabad" },
  { customer_id: 205, name: "arjun mehta", email: "arjun@example.com", city: "mumbai" }
])

// insert sample data into orders collection

db.orders.insertmany([
  { order_id: 301, customer_id: 201, book_id: 101, order_date: isodate("2023-01-15"), quantity: 1 },
  { order_id: 302, customer_id: 202, book_id: 104, order_date: isodate("2023-03-12"), quantity: 2 },
  { order_id: 303, customer_id: 203, book_id: 102, order_date: isodate("2022-12-20"), quantity: 1 },
  { order_id: 304, customer_id: 204, book_id: 105, order_date: isodate("2023-06-10"), quantity: 3 },
  { order_id: 305, customer_id: 205, book_id: 103, order_date: isodate("2023-02-28"), quantity: 1 },
  { order_id: 306, customer_id: 202, book_id: 101, order_date: isodate("2023-05-05"), quantity: 1 },
  { order_id: 307, customer_id: 202, book_id: 105, order_date: isodate("2023-07-15"), quantity: 1 }
])

// list all books priced above 500

db.books.find({ price: { $gt: 500 } })

// show all customers from hyderabad

db.customers.find({ city: "hyderabad" })

// find all orders placed after jan 1, 2023

db.orders.find({ order_date: { $gt: isodate("2023-01-01") } })

// order details with customer name and book title

db.orders.aggregate([
  {
    $lookup: {
      from: "customers",
      localfield: "customer_id",
      foreignfield: "customer_id",
      as: "customer_info"
    }
  },
  { $unwind: "$customer_info" },
  {
    $lookup: {
      from: "books",
      localfield: "book_id",
      foreignfield: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $project: {
      _id: 0,
      order_id: 1,
      customer_name: "$customer_info.name",
      book_title: "$book_info.title",
      quantity: 1,
      order_date: 1
    }
  }
])

// total quantity ordered for each book

db.orders.aggregate([
  {
    $group: {
      _id: "$book_id",
      total_quantity: { $sum: "$quantity" }
    }
  },
  {
    $lookup: {
      from: "books",
      localfield: "_id",
      foreignfield: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $project: {
      _id: 0,
      book_title: "$book_info.title",
      total_quantity: 1
    }
  }
])

// total number of orders placed by each customer

db.orders.aggregate([
  {
    $group: {
      _id: "$customer_id",
      order_count: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "customers",
      localfield: "_id",
      foreignfield: "customer_id",
      as: "customer_info"
    }
  },
  { $unwind: "$customer_info" },
  {
    $project: {
      _id: 0,
      customer_name: "$customer_info.name",
      order_count: 1
    }
  }
])

// total revenue generated per book

db.orders.aggregate([
  {
    $lookup: {
      from: "books",
      localfield: "book_id",
      foreignfield: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $project: {
      book_id: 1,
      title: "$book_info.title",
      revenue: { $multiply: ["$quantity", "$book_info.price"] }
    }
  },
  {
    $group: {
      _id: "$book_id",
      title: { $first: "$title" },
      total_revenue: { $sum: "$revenue" }
    }
  }
])

// find the book with the highest total revenue

db.orders.aggregate([
  {
    $lookup: {
      from: "books",
      localfield: "book_id",
      foreignfield: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $project: {
      title: "$book_info.title",
      revenue: { $multiply: ["$quantity", "$book_info.price"] }
    }
  },
  {
    $group: {
      _id: "$title",
      total_revenue: { $sum: "$revenue" }
    }
  },
  { $sort: { total_revenue: -1 } },
  { $limit: 1 }
])

// genres and total books sold in each genre

db.orders.aggregate([
  {
    $lookup: {
      from: "books",
      localfield: "book_id",
      foreignfield: "book_id",
      as: "book_info"
    }
  },
  { $unwind: "$book_info" },
  {
    $group: {
      _id: "$book_info.genre",
      total_sold: { $sum: "$quantity" }
    }
  },
  {
    $project: {
      genre: "$_id",
      total_sold: 1,
      _id: 0
    }
  }
])

// customers who ordered more than 2 different books

db.orders.aggregate([
  {
    $group: {
      _id: { customer_id: "$customer_id", book_id: "$book_id" }
    }
  },
  {
    $group: {
      _id: "$_id.customer_id",
      book_count: { $sum: 1 }
    }
  },
  { $match: { book_count: { $gt: 2 } } },
  {
    $lookup: {
      from: "customers",
      localfield: "_id",
      foreignfield: "customer_id",
      as: "customer_info"
    }
  },
  { $unwind: "$customer_info" },
  {
    $project: {
      _id: 0,
      customer_name: "$customer_info.name",
      book_count: 1
    }
  }
])
