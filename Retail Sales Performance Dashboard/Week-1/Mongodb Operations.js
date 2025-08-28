db.campaign_feedback.insertOne(
  {
productID: 1,
storeID: 1,
campaignName: "Sale-2k25",
startDate: new Date("2025-06-01"),
endDate: new Date("2025-06-15"),
feedback: [
  {
		customerID: 1,
		rating: 4,
		comment: "Nice",
		timestamp: new Date("2025-06-02")
	},
  {
		customerID: 2,
		rating: 5,
		comment: "Good offers",
		timestamp: new Date("2025-06-03")
	}
]
	}
)

db.campaign_feedback.createIndex({ product_id: 1 })

db.campaign_feedback.createIndex({ store_id: 1 })

db.campaign_feedback.createIndex({ campaign_id: 1 })
