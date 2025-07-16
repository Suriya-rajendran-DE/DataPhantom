from pyspark.sql import SparkSession
import subprocess

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ArchiveRawAndStaging") \
    .enableHiveSupport() \
    .getOrCreate()

# Define paths
raw_path = "hdfs://localhost:9000/raw_tbls/"
archive_path = "hdfs://localhost:9000/data/archived/"
stg_path = "hdfs://localhost:9000/stg_tbls/"

# Read raw data
df_raw = spark.read.parquet(raw_path + "spark_raw_tbl")

# Archive the data
df_raw.write.mode("overwrite").parquet(archive_path + "spark_raw_tbl")
print("Raw data archived to: hdfs://localhost:9000/data/archived/spark_raw_tbl")

# Delete raw and staging HDFS folders using subprocess
try:
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", raw_path], check=True)
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", stg_path], check=True)
    print("Raw table and staging table paths deleted.")
except subprocess.CalledProcessError as e:
    print(f"Failed to delete directories: {e}")