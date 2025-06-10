from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Quality Check") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Read staged data
df = spark.sql("SELECT * FROM stage_emp_temp_table")

# Perform data quality 
print("\nRunning Data Quality Checks...\n")

# 1. Check for Nulls in All Columns
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
#print(f"\nNull_Counts: {null_counts}")


# 2. Check for Duplicates
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"\nDuplicate Rows: {duplicate_count}")

# 3. Validate Unique Employee IDs
distinct_department_ids = df.select(countDistinct("dept_id")).collect()[0][0]
total_rows = df.count()
print(f"\nUnique Department IDs: {distinct_department_ids} out of {total_rows} rows")


# 4. Check Valid Gender Values (Expecting only 'M' or 'F')
invalid_gender = df.filter(~col("gender").isin(["M", "F"])).count()
print(f"\nInvalid Gender Values Found: {invalid_gender}")


# 5. Validate Salary & Age Ranges
invalid_salary = df.filter(col("salary") < 0).count()
invalid_age = df.filter((col("age") < 18) | (col("age") > 65)).count()
print(f"\nNegative Salary Entries: {invalid_salary}")
print(f"\nEmployees Outside Expected Age Range (18-65): {invalid_age}")



# Load cleaned data into final Hive output table
df.write.mode("overwrite").format("hive").saveAsTable("emp_output_table")

print("\nData has been loaded into output table successfully\n")

time.sleep(1800)  # Keeps the SparkContext alive for 5 minutes

spark.stop()