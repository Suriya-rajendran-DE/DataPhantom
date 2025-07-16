from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark
spark = SparkSession.builder \
    .appName("DataQualityChecks") \
    .enableHiveSupport() \
    .getOrCreate()

# Read staging data
df_stg = spark.read.parquet("hdfs://localhost:9000/stg_tbls/")

# Data Quality Checks
dq_checked_df = df_stg.filter(
    (col("Product_Category").isNotNull()) &
    (col("Age_Group").isNotNull()) &
    (col("Total_Revenue") > 0) &
    (col("Avg_Price") > 0) &
    (col("Unique_Customers") > 0) &
    (col("Transaction_Count") > 0)
)

# Optional: show a preview and count records filtered out
print(f"Records before DQ checks: {df_stg.count()}")
print(f"Records after DQ checks: {dq_checked_df.count()}")

# Save the clean data to final HDFS location
dq_checked_df.write.mode("overwrite").parquet("hdfs://localhost:9000/final_tbls/")

