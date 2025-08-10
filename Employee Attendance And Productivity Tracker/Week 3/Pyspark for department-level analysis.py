from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, when, avg

spark = SparkSession.builder.appName("AttendanceAnalysis").getOrCreate()

# load data
df = spark.read.csv("attendance_log.csv", header=True, inferSchema=True)

# convert clock-in/out to timestamps
df = df.withColumn("clockin", to_timestamp("clockin")) \
       .withColumn("clockout", to_timestamp("clockout"))

# calculate work hours
df = df.withColumn("work_hours", (col("clockout").cast("long") - col("clockin").cast("long")) / 3600)

# identify late logins (after 9:30 am)
df = df.withColumn("late_login", when(hour("clockin") > 9, 1).otherwise(0))

# group by department
summary = df.groupBy("department") \
            .agg(avg("work_hours").alias("avg_hours"),
                 avg("late_login").alias("avg_late_logins"))

summary.show()

# save results
summary.write.csv("department_attendance_summary.csv", header=True)
