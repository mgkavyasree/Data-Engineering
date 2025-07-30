// use retail database
use retail_db;

// insert campaign feedback
db.campaigns.insertMany([
  {
    campaign_id: 1,
    store_id: 101,
    product_id: 1,
    feedback: "positive",
    comments: "good response"
  },
  {
    campaign_id: 2,
    store_id: 102,
    product_id: 2,
    feedback: "negative",
    comments: "low visibility"
  }
]);

// create indexes for fast search
db.campaigns.createIndex({ product_id: 1 });
db.campaigns.createIndex({ store_id: 1 });
