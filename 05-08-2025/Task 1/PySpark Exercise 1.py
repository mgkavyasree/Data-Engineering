from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# 1. spark setup & data load
spark = SparkSession.builder \
    .appName("BotCampus Intermediate Session") \
    .master("local[*]") \
    .getOrCreate()

data = [("ananya", "bangalore", 24),
        ("ravi", "hyderabad", 28),
        ("kavya", "delhi", 22),
        ("meena", "chennai", 25)]
columns = ["name", "city", "age"]
df = spark.createDataFrame(data, columns)
df.show()

# 2. rdd transformations
feedback = spark.sparkContext.parallelize([
    "Ravi from Bangalore loved the mobile app",
    "Meena from Delhi reported poor response time",
    "Ajay from Pune liked the delivery speed",
    "Ananya from Hyderabad had an issue with UI",
    "Rohit from Mumbai gave positive feedback"
])

words_rdd = feedback.flatMap(lambda line: line.lower().split())
print("total words:", words_rdd.count())

from collections import Counter
word_counts = words_rdd.countByValue()
top_3_words = Counter(word_counts).most_common(3)
print("top 3 words:", top_3_words)

stop_words = {"from", "with", "the", "had", "an", "and"}
filtered_words = words_rdd.filter(lambda word: word not in stop_words)
filtered_word_counts = filtered_words.countByValue()
print("filtered word counts:", dict(filtered_word_counts))

# 3. dataframe transformations
scores = [
    ("ravi", "math", 88),
    ("ananya", "science", 92),
    ("kavya", "english", 79),
    ("ravi", "english", 67),
    ("neha", "math", 94),
    ("meena", "science", 85)
]
columns = ["name", "subject", "score"]
df_scores = spark.createDataFrame(scores, columns)

df_scores = df_scores.withColumn(
    "grade",
    when(col("score") >= 90, "a")
    .when((col("score") >= 80), "b")
    .when((col("score") >= 70), "c")
    .otherwise("d")
)

df_scores.groupBy("subject").agg(avg("score").alias("avg_score")).show()

df_scores = df_scores.withColumn(
    "difficulty",
    when(col("subject").isin("math", "science"), "difficult").otherwise("easy")
)

window_spec = Window.partitionBy("subject").orderBy(col("score").desc())
df_scores = df_scores.withColumn("rank", rank().over(window_spec))

df_scores = df_scores.withColumn("name_upper", upper(col("name")))

# 4. load csv & json 
students = spark.read.option("header", True).csv("students.csv")
employees = spark.read.option("multiline", True).json("employee_nested.json")

employees_flat = employees.select(
    "id", "name",
    col("address.city").alias("city"),
    col("address.pincode").alias("pincode"),
    explode("skills").alias("skill")
)

students.write.mode("overwrite").parquet("/tmp/output/students")
employees_flat.write.mode("overwrite").parquet("/tmp/output/employees")

# 5. spark sql & attendance
df_scores.createOrReplaceTempView("exam_scores")

spark.sql("SELECT subject, name, score FROM (SELECT *, RANK() OVER (PARTITION BY subject ORDER BY score DESC) as rnk FROM exam_scores) WHERE rnk = 1").show()
spark.sql("SELECT grade, COUNT(*) FROM exam_scores GROUP BY grade").show()
spark.sql("SELECT name, COUNT(*) as cnt FROM exam_scores GROUP BY name HAVING cnt > 1").show()
spark.sql("SELECT subject, AVG(score) as avg FROM exam_scores GROUP BY subject HAVING avg > 85").show()

attendance = spark.createDataFrame([
    ("ravi", 18),
    ("ananya", 22),
    ("kavya", 25),
    ("neha", 20),
    ("meena", 17)
], ["name", "days_present"])

df_scores = df_scores.join(attendance, "name", "left")

df_scores = df_scores.withColumn(
    "adj_grade",
    when(col("days_present") < 20,
         when(col("grade") == "a", "b")
         .when(col("grade") == "b", "c")
         .when(col("grade") == "c", "d")
         .otherwise("d")).otherwise(col("grade"))
)

# 6. partitioned load
df_scores.write.mode("overwrite").partitionBy("subject").parquet("/tmp/scores/")

df_inc = spark.createDataFrame([("meena", "math", 93)], ["name", "subject", "score"])
df_inc.write.mode("append").partitionBy("subject").parquet("/tmp/scores/")

# 7. etl clean & transform
df_raw = spark.read.option("header", True).csv("raw_emp.csv")
df_raw = df_raw.withColumn("salary", col("salary").cast("int"))
df_raw = df_raw.withColumn("bonus", coalesce(col("bonus").cast("int"), lit(2000)))
df_raw = df_raw.withColumn("total_ctc", col("salary") + col("bonus"))
df_clean = df_raw.filter(col("total_ctc") > 60000)
df_clean.write.mode("overwrite").parquet("emp_final.parquet")
df_clean.write.mode("overwrite").json("emp_final.json")
