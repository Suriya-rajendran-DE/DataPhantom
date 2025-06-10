

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transform Data") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Read data from raw table
raw_data = spark.sql("SELECT * FROM raw_emp_temp_table")

# Perform transformations
transformed_data = raw_data.withColumn("transformed_value", col("value") * 2)

# Write to stage table
transformed_data.write.mode("overwrite").format("hive").saveAsTable("stage_emp_temp_table")

spark.stop()