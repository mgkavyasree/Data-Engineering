from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Module 1: Setup & SparkSession

spark = SparkSession.builder \
    .appName("BotCampus PySpark Practice") \
    .master("local[*]") \
    .getOrCreate()

data = [("Anjali", "Bangalore", 24),
        ("Ravi", "Hyderabad", 28),
        ("Kavya", "Delhi", 22),
        ("Meena", "Chennai", 25),
        ("Arjun", "Mumbai", 30)]
columns = ["name", "city", "age"]
df = spark.createDataFrame(data, columns)
df.printSchema()
df.show()

print("Data types:", df.dtypes)
print("To RDD:", df.rdd.collect())
print("Mapped RDD:", df.rdd.map(lambda row: (row.name, row.city)).collect())

# Module 2: RDD Transformations

feedback = spark.sparkContext.parallelize([
    "Ravi from Bangalore loved the delivery",
    "Meena from Hyderabad had a late order",
    "Ajay from Pune liked the service",
    "Anjali from Delhi faced UI issues",
    "Rohit from Mumbai gave positive feedback"
])

stop_words = {"from", "the", "a", "had"}
words = feedback.flatMap(lambda line: line.lower().split())                 .filter(lambda word: word not in stop_words)                 .map(lambda word: (word, 1))                 .reduceByKey(lambda a, b: a + b)
top_3 = words.takeOrdered(3, key=lambda x: -x[1])
print("Top 3 frequent words:", top_3)

# Module 3: DataFrame Joins

students = [("Amit", "10-A", 89),
            ("Kavya", "10-B", 92),
            ("Anjali", "10-A", 78),
            ("Rohit", "10-B", 85),
            ("Sneha", "10-C", 80)]
attendance = [("Amit", 24),
              ("Kavya", 22),
              ("Anjali", 20),
              ("Rohit", 25),
              ("Sneha", 19)]
columns1 = ["name", "section", "marks"]
columns2 = ["name", "days_present"]
df_students = spark.createDataFrame(students, columns1)
df_attendance = spark.createDataFrame(attendance, columns2)
df_joined = df_students.join(df_attendance, "name")
df_joined = df_joined.withColumn("attendance_rate", col("days_present") / 25)

df_joined = df_joined.withColumn("grade",
    when(col("marks") > 90, "A")
    .when((col("marks") > 80), "B")
    .otherwise("C")
)
df_joined.filter((col("grade").isin("A", "B")) & (col("attendance_rate") < 0.8)).show()

# Module 4: Ingest CSV & JSON

emp_csv = spark.read.option("header", True).csv("employees.csv")
emp_json = spark.read.option("multiline", True).json("employee_nested.json")

emp_flat = emp_json.select(
    col("id"), col("name"),
    col("contact.city").alias("city"),
    col("contact.email").alias("email"),
    explode(col("skills")).alias("skill")
)

emp_csv.write.mode("overwrite").partitionBy("city").parquet("output/employees_csv")
emp_flat.write.mode("overwrite").partitionBy("city").parquet("output/employees_json")

# Module 5: Spark SQL Queries

df_students.createOrReplaceTempView("students_view")

spark.sql("SELECT section, AVG(marks) as avg_marks FROM students_view GROUP BY section").show()

spark.sql("""
SELECT section, name, marks FROM (
  SELECT *, RANK() OVER(PARTITION BY section ORDER BY marks DESC) as rnk
  FROM students_view
) WHERE rnk = 1
""").show()

spark.sql("""
SELECT CASE 
  WHEN marks > 90 THEN 'A' 
  WHEN marks > 80 THEN 'B' 
  ELSE 'C' 
END as grade, COUNT(*) as count 
FROM students_view 
GROUP BY grade
""").show()

spark.sql("""
SELECT * FROM students_view WHERE marks > (
  SELECT AVG(marks) FROM students_view
)
""").show()

# Module 6: Partitioned Load

df_students.write.mode("overwrite").partitionBy("section").parquet("output/students/")
df_inc = spark.createDataFrame([("Tejas", "10-A", 91)], ["name", "section", "marks"])
df_inc.write.mode("append").partitionBy("section").parquet("output/students/")

import os
print("Files in output/students/:", os.listdir("output/students/section=10-A"))
df_10a = spark.read.parquet("output/students/section=10-A")
print("10-A count:", df_10a.count())

# Module 7: ETL End to End

etl_df = spark.read.option("header", True).csv("etl_raw.csv", inferSchema=True)
etl_df = etl_df.withColumn("bonus", coalesce(col("bonus"), lit(2000)))
etl_df = etl_df.withColumn("total_ctc", col("salary") + col("bonus"))
etl_filtered = etl_df.filter(col("total_ctc") > 65000)

etl_filtered.write.mode("overwrite").json("output/etl_json")
etl_filtered.write.mode("overwrite").partitionBy("dept").parquet("output/etl_parquet")
