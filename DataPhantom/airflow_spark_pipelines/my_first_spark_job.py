from pyspark.sql import SparkSession
from pyspark.sql.types import _parse_datatype_string
from pyspark.sql.functions import to_date, current_date, current_timestamp, col, when, count, sum, avg
import logging
import time

# Create Spark session
spark = (
    SparkSession.builder
    .appName("My_first_spark_job_submission")
    .master("local[*]")
    .getOrCreate()
)

print("\nspark job is created\n")
print("spark version used is :" + spark.sparkContext.version)

# Define schema
emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"
df = spark.read.format("csv").option("header", True).schema(emp_schema).load("hdfs://localhost:9000/datasets/emp.csv")

print("input data has been read successfully")

# Convert hire_date properly
df = df.withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))

# Rename columns
df1 = df.toDF("emp_id", "dept_id", "emp_name", "emp_age", "emp_gender", "emp_salary", "emp_hireData")

print("\nSchema has been successfully changed\n")

# Modify gender values
df2 = df1.withColumn("gender_short", when(col("emp_gender") == 'Male', 'M')
                                .when(col("emp_gender") == 'Female', 'F')
                                .otherwise(None))

# Add tax column
df3 = df2.withColumn("emp_tax", col("emp_salary") * 0.2)

# Perform aggregation
df4 = df3.groupBy("dept_id").agg(
    count("emp_id").alias("no_of_emp_each_dept"),
    sum("emp_salary").alias("total_dept_salary"),
    avg("emp_salary").alias("avg_dept_salary"),
    sum("emp_tax").alias("total_dept_tax")
)

# Add current date and timestamp
# df5 = df4.withColumn("current_data", current_date()) \
#          .withColumn("current_timestamp", current_timestamp())

df5 = df4.withColumn("current_date", current_date()).withColumn("timestamp", current_timestamp())

# Select relevant columns
#df6 = df5.select("dept_id", "no_of_emp_each_dept", "total_dept_salary", "avg_dept_salary")

# Check for null values
# df7 = df6.select([col(c).isNull().alias(c) for c in df6.columns]) \
#        .groupBy().agg(*[sum(col(c).cast("int")).alias(c) for c in df6.columns])

#logging.basicConfig(level=logging.INFO)
#logging.info(df5.collect())  # Correct way to log DataFrame content

first_row = df5.first()

# Fetch the current date from the specific column
current_date = first_row["current_date"]

#print("Current Date from First Row:", current_date)
logging.info(f"Current Date: {current_date}")

# Fill null values with 0
#df8 = df7.fillna(0)

print("\nTransformation has been completed successfully\n")

# Save output
df5.write.format("csv").mode("overwrite").option("header", True).save("hdfs://localhost:9000/datasets/emp_output/")

print("\nData has been loaded successfully\n")

spark.stop()

#time.sleep(3600)  # Keeps the SparkContext alive for 5 minutes