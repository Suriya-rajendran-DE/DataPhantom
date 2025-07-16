from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestLocalMode") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session started")
print(spark.version)
spark.stop()