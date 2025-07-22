// Retail Store Inventory & Sales 
use retailstoreDB;

// Create Collection: products

db.products.insertMany([
  { product_id: 101, name: "Laptop", category: "Electronics", price: 55000, stock: 30 },
  { product_id: 102, name: "Mobile", category: "Electronics", price: 25000, stock: 50 },
  { product_id: 103, name: "Chair", category: "Furniture", price: 3000, stock: 100 },
  { product_id: 104, name: "Desk", category: "Furniture", price: 7000, stock: 40 },
  { product_id: 105, name: "Book", category: "Stationery", price: 250, stock: 200 }
]);

// Create Collection: sales 

db.sales.insertMany([
  { sale_id: 1, product_id: 101, quantity: 2, date: new Date("2024-08-10"), customer: "Ravi" },
  { sale_id: 2, product_id: 102, quantity: 3, date: new Date("2024-08-12"), customer: "Ayesha" },
  { sale_id: 3, product_id: 103, quantity: 5, date: new Date("2024-08-14"), customer: "Ravi" },
  { sale_id: 4, product_id: 104, quantity: 1, date: new Date("2024-08-14"), customer: "John" },
  { sale_id: 5, product_id: 105, quantity: 10, date: new Date("2024-08-15"), customer: "Meena" }
]);

// Find all products in the Electronics category.

db.products.find({ category: "Electronics" });

// List all sales made by Ravi.

db.sales.find({ customer: "Ravi" });

// Get details of products whose price is more than 5000.

db.products.find({ price: { $gt: 5000 } });

// Find all products with stock less than 50.

db.products.find({ stock: { $lt: 50 } });

// Show all products sold on 2024-08-14.

db.sales.find({ date: new Date("2024-08-14") });

// Aggregation & Join-style Operations

// For each product, show total quantity sold.

db.sales.aggregate([
  {
    $group: {
      _id: "$product_id",
      total_quantity_sold: { $sum: "$quantity" }
    }
  }
]);

// Display the total revenue (price × quantity) generated per product.

db.sales.aggregate([
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
      _id: "$product_id",
      product_name: { $first: "$product_info.name" },
      total_revenue: { $sum: { $multiply: ["$quantity", "$product_info.price"] } }
    }
  }
]);

// List customers who purchased more than 3 items in any sale.

db.sales.find({ quantity: { $gt: 3 } });

// Sort products by stock in descending order.

db.products.find().sort({ stock: -1 });

// Find the top 2 best-selling products based on quantity.

db.sales.aggregate([
  {
    $group: {
      _id: "$product_id",
      total_quantity: { $sum: "$quantity" }
    }
  },
  { $sort: { total_quantity: -1 } },
  { $limit: 2 }
]);
