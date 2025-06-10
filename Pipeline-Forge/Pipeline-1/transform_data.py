
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import _parse_datatype_string
from pyspark.sql.functions import to_date, current_date, current_timestamp, col, when, count, sum, avg
import logging
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transform Data") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Read data from raw table
raw_data = spark.sql("SELECT * FROM raw_emp_temp_table")

print("\nData has been extracted from from raw table successfully\n")

# Perform transformations --

# Convert hire_date properly
df1 = raw_data.withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))

# Rename columns
df2 = df1.toDF("sn_id","emp_id", "dept_id", "emp_name", "emp_age", "emp_gender", "emp_salary", "emp_hireData")

print("\nSchema has been successfully changed\n")

# Modify gender values
df3 = df2.withColumn("gender_short", when(col("emp_gender") == 'Male', 'M')
                                .when(col("emp_gender") == 'Female', 'F')
                                .otherwise(None))

# Add tax column
df4 = df3.withColumn("emp_tax", col("emp_salary") * 0.2)

# Perform aggregation
df5 = df4.groupBy("dept_id").agg(
    count("emp_id").alias("no_of_emp_each_dept"),
    sum("emp_salary").alias("total_dept_salary"),
    avg("emp_salary").alias("avg_dept_salary"),
    sum("emp_tax").alias("total_dept_tax")
)

df6 = df5.withColumn("salary", when(rand() > 0.8, None).otherwise(col("emp_salary"))) \
         .withColumn("gender", when(rand() > 0.9, None).otherwise(col("emp_gender")))

# Add current date and timestamp
transformed_data = df6.withColumn("current_date", current_date()).withColumn("timestamp", current_timestamp())

first_row = transformed_data.first()

# Fetch the current date from the specific column
current_date = first_row["current_date"]

#print("Current Date from First Row:", current_date)
logging.info(f"Current Date: {current_date}")

print("\nTransformation has been completed successfully\n")

# Write to stage table
transformed_data.write.mode("overwrite").format("hive").saveAsTable("stage_emp_temp_table")

print("\nData has been loaded into stage table successfully\n")

time.sleep(1800)  # Keeps the SparkContext alive for 5 minutes

spark.stop()