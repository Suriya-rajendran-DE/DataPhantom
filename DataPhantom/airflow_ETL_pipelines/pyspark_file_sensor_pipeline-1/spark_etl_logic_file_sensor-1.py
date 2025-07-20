from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, to_date
import sys

if __name__ == "__main__":
    input_path = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("EmployeeETLJob")
        .master("local[*]")
        .getOrCreate()
    )

    # Load CSV with header
    df = spark.read.option("header", True).csv(input_path)

    print(f"[INFO] Original data shape: {df.count()} rows × {len(df.columns)} columns")

    # Basic cleaning
    df_cleaned = df.dropDuplicates()

    df_cleaned = df_cleaned.fillna({
        "salary": "0",
        "gender": "unknown",
        "name": "no_name"
    })

    # Column normalization
    for col_name in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumnRenamed(col_name, col_name.lower().strip())

    # Date conversion
    if "hire_date" in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))

    print("[INFO] Transformations applied successfully")
    print(f"[INFO] Final data shape: {df_cleaned.count()} rows × {len(df_cleaned.columns)} columns")

    # Save to output location
    df_cleaned.write.mode("overwrite").option("header", True).csv("/mnt/d/Airflow_Dags_Scripts/FILE_SNSR_DAGS/file_sensor/file_sensor-1/output/file-sensor-1")

    spark.stop()