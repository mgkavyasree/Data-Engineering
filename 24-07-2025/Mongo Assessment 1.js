// E-Commerce Store Analytics
// products collection
db.products.insertMany([
  { product_id: 1001, name: "wireless mouse", category: "electronics", price: 750, stock: 120 },
  { product_id: 1002, name: "bluetooth speaker", category: "electronics", price: 2200, stock: 80 },
  { product_id: 1003, name: "yoga mat", category: "fitness", price: 599, stock: 150 },
  { product_id: 1004, name: "office chair", category: "furniture", price: 7500, stock: 40 },
  { product_id: 1005, name: "running shoes", category: "footwear", price: 3500, stock: 60 }
])

// orders collection
db.orders.insertMany([
  { order_id: 5001, customer: "ravi shah", product_id: 1001, quantity: 2, order_date: new Date("2024-07-01") },
  { order_id: 5002, customer: "sneha mehta", product_id: 1002, quantity: 1, order_date: new Date("2024-07-02") },
  { order_id: 5003, customer: "arjun verma", product_id: 1003, quantity: 3, order_date: new Date("2024-07-03") },
  { order_id: 5004, customer: "neha iyer", product_id: 1001, quantity: 1, order_date: new Date("2024-07-04") },
  { order_id: 5005, customer: "mohit jain", product_id: 1005, quantity: 2, order_date: new Date("2024-07-05") }
])

// list all products in the electronics category
db.products.find({ category: "electronics" })

// find all orders placed by ravi shah
db.orders.find({ customer: "ravi shah" })

// show all orders placed after july 2, 2024
db.orders.find({ order_date: { $gt: new Date("2024-07-02") } })

// display the product with stock less than 50
db.products.find({ stock: { $lt: 50 } })

// show all products that cost more than 2000
db.products.find({ price: { $gt: 2000 } })

// join queries with $lookup
// use $lookup to show each order with the product name and price
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },
  { $unwind: "$product_info" },
  {
    $project: {
      order_id: 1,
      customer: 1,
      order_date: 1,
      quantity: 1,
      product_name: "$product_info.name",
      product_price: "$product_info.price"
    }
  }
])

// find total amount spent by each customer (price × quantity)
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },
  { $unwind: "$product_info" },
  {
    $group: {
      _id: "$customer",
      total_spent: { $sum: { $multiply: ["$quantity", "$product_info.price"] } }
    }
  }
])

// list all orders along with category of the product
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },
  { $unwind: "$product_info" },
  {
    $project: {
      order_id: 1,
      customer: 1,
      order_date: 1,
      category: "$product_info.category"
    }
  }
])

// find customers who ordered any fitness product
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },
  { $unwind: "$product_info" },
  { $match: { "product_info.category": "fitness" } },
  { $group: { _id: "$customer" } }
])

// find the total sales per product category
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },
  { $unwind: "$product_info" },
  {
    $group: {
      _id: "$product_info.category",
      total_sales: { $sum: { $multiply: ["$quantity", "$product_info.price"] } }
    }
  }
])

// aggregations & grouping
// count how many units of each product have been sold
db.orders.aggregate([
  {
    $group: {
      _id: "$product_id",
      total_units_sold: { $sum: "$quantity" }
    }
  }
])

// calculate average price of products per category
db.products.aggregate([
  {
    $group: {
      _id: "$category",
      average_price: { $avg: "$price" }
    }
  }
])

// find out which customer made the largest single order (by amount)
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },
  { $unwind: "$product_info" },
  {
    $project: {
      customer: 1,
      order_amount: { $multiply: ["$quantity", "$product_info.price"] }
    }
  },
  { $sort: { order_amount: -1 } },
  { $limit: 1 }
])

// list the top 3 products based on number of orders
db.orders.aggregate([
  {
    $group: {
      _id: "$product_id",
      order_count: { $sum: 1 }
    }
  },
  { $sort: { order_count: -1 } },
  { $limit: 3 }
])

// determine which day had the highest number of orders
db.orders.aggregate([
  {
    $group: {
      _id: "$order_date",
      orders_count: { $sum: 1 }
    }
  },
  { $sort: { orders_count: -1 } },
  { $limit: 1 }
])

// add a new customer who hasn't placed any orders
db.customers.insertOne({ name: "kavya sree", email: "kavya@example.com" })

// list customers without orders (simulate this)
db.customers.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "name",
      foreignField: "customer",
      as: "orders"
    }
  },
  { $match: { orders: { $eq: [] } } }
])

// add more orders and find customers who have placed more than one order
db.orders.insertMany([
  { order_id: 5006, customer: "ravi shah", product_id: 1003, quantity: 1, order_date: new Date("2024-07-06") },
  { order_id: 5007, customer: "arjun verma", product_id: 1005, quantity: 2, order_date: new Date("2024-07-07") }
])

db.orders.aggregate([
  {
    $group: {
      _id: "$customer",
      order_count: { $sum: 1 }
    }
  },
  { $match: { order_count: { $gt: 1 } } }
])

// find all products that have never been ordered
db.products.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "product_id",
      foreignField: "product_id",
      as: "order_info"
    }
  },
  { $match: { order_info: { $eq: [] } } }
])

// display customers who placed orders for products with stock less than 100
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },
  { $unwind: "$product_info" },
  { $match: { "product_info.stock": { $lt: 100 } } },
  { $group: { _id: "$customer" } }
])

// show the total inventory value (price × stock) for all products
db.products.aggregate([
  {
    $group: {
      _id: null,
      total_inventory_value: { $sum: { $multiply: ["$price", "$stock"] } }
    }
  }
])
