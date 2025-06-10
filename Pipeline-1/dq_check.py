from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Quality Check") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Read staged data
stage_data = spark.sql("SELECT * FROM stage_emp_temp_table")

# Perform data quality checks
dq_results = stage_data.filter(stage_data.transformed_value.isNotNull())

# Load cleaned data into final Hive output table
dq_results.write.mode("overwrite").format("hive").saveAsTable("emp_output_table")

spark.stop()