from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveTableCreation") \
    .enableHiveSupport() \
    .getOrCreate()

# Read Parquet data from HDFS
df_raw = spark.read.parquet("hdfs:///data/landing/retail_sales_raw")

# Register DataFrame as a temporary view
df_raw.createOrReplaceTempView("raw_view")

# Create Hive table if not exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS spark_raw_tbl (
        Transaction_ID INT,
        Date DATE,
        Customer_ID STRING,
        Gender STRING,
        Age INT,
        Product_Category STRING,
        Quantity INT,
        Price_per_Unit INT,
        Total_Amount INT
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/raw_tbls/';
""")

# Insert data into the Hive table
spark.sql("""
    INSERT OVERWRITE TABLE spark_raw_tbl
    SELECT * FROM raw_view
""")



df_transformed.createOrReplaceTempView("stg_view")

spark.sql("""
    CREATE TABLE IF NOT EXISTS spark_stg_tbl (
        Product_Category STRING,
        Age_Group STRING,
        Total_Revenue DOUBLE,
        Avg_Price DOUBLE,
        Unique_Customers INT,
        Transaction_Count INT
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/stg_tbls/';
""")

spark.sql("""
    INSERT OVERWRITE TABLE spark_stg_tbl
    SELECT * FROM stg_view
""")