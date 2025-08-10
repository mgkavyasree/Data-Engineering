from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, avg

spark = SparkSession.builder.appName("RetailInsights").getOrCreate()

# load dataset
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# filter underperforming products
underperforming = df.filter((col("quantity") < 5) & (col("returns") > 0))

# average monthly revenue by store
df = df.withColumn("month", month("sale_date"))
monthly_avg = df.groupBy("store_id", "month") \
    .agg(avg(col("price") * col("quantity")).alias("avg_monthly_revenue"))

# show results
underperforming.show()
monthly_avg.show()

# save output
underperforming.write.csv("underperforming_products.csv", header=True)
monthly_avg.write.csv("monthly_avg_revenue.csv", header=True)
