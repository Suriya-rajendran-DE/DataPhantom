from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("create_hive_tables") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Print Spark version for validation
print(spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())

# Drop table if it exists (optional)
spark.sql("DROP TABLE IF EXISTS raw_emp_temp_table")

# Create raw table with corrected column types
spark.sql("""
CREATE OR REPLACE TABLE raw_emp_temp_table (
    id INT,
    employee_id INT,
    department_id INT,
    name STRING,
    age INT,
    gender STRING,
    salary DECIMAL(10,2),
    hire_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');  -- Skips header row if present
""")

# Load CSV data into raw table from HDFS (Wrapped in spark.sql)
spark.sql("""
LOAD DATA INPATH 'hdfs://localhost:9000/datasets/emp.csv' INTO TABLE raw_emp_temp_table;
""")

# Create stage table
spark.sql("""
CREATE OR REPLACE TABLE stage_emp_temp_table (
    sn_id INT,
    emp_id INT,
    dept_id INT,
    emp_name STRING,
    emp_age INT,
    emp_gender STRING,
    emp_salary DOUBLE,
    emp_hireData DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
""")

print("\nHive tables created successfully!")