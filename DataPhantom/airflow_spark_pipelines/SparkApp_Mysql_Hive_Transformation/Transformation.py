from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, countDistinct, count, when, round, dense_rank
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import min, max
from pyspark.sql.functions import round
from pyspark.sql.window import Window



spark = SparkSession.builder \
    .appName("RetailDataTransformation") \
    .enableHiveSupport() \
    .getOrCreate()

# Read raw data from HDFS location
df_raw = spark.read.parquet("hdfs://localhost:9000/raw_tbls/")

df = df_raw.toDF("Transaction_Id", "Date", "Customer_Id", "Gender", "Age", "Product_Category", "Quantity", "Price_Per_unit", "Total_Amount")

### **1. Date Transformations**
#- **Extract Year, Month, Day:** 
df = df.withColumn("Year", year(df["Date"]))
df = df.withColumn("Month", month(df["Date"]))
df = df.withColumn("Day", dayofmonth(df["Date"]))


df = df.withColumn("Age_Group", when(col("Age") < 20, "Teenage")
                                .when((col("Age") >= 20) & (col("Age") <= 40), "Yougnger_Adult")
                                .when((col("Age") >= 40) & (col("age") < 60), "Middle_Aged")
                                .otherwise("Senior"))



#Price Normalization

min_price = df.select(min(col("Price_Per_unit"))).collect()[0][0]
max_price = df.select(max(col("Price_Per_unit"))).collect()[0][0]

df = df.withColumn("Price_Normalized", (col("Price_Per_unit") - min_price) / (max_price - min_price))

df = df.withColumn("Price_Normalized", round(col("Price_Normalized"), 2))

df = df.drop("Rounded_Value")

df = df.withColumn("Total_Amount", col("Quantity") * col("Price_per_Unit"))

#Window Functions for Advanced Analytics**
window_spec = Window.partitionBy("Customer_Id").orderBy("Date").rowsBetween(-2, 0)
df = df.withColumn("Moving_Avg_Sales", avg("Total_Amount").over(window_spec))

#Rank Transactions by High-Value Customers**
window_spec = Window.partitionBy("Customer_Id").orderBy(df["Total_Amount"].desc())
df = df.withColumn("Transaction_Rank", dense_rank().over(window_spec))

#top-Selling Product Categories

df_grouped = df.groupBy("Product_Category", "Age_Group").agg(
    sum(col("Total_Amount").cast("float")).alias("Total_Revenue"),
    avg(col("Price_Per_unit")).alias("Avg_Price"),
    countDistinct(col("Customer_Id")).alias("Unique_Customers"),
    count(col("Transaction_Id")).alias("Transaction_Count")
).orderBy(col("Total_Revenue").desc())


df_aggregated = df_grouped.withColumn("Avg_Price", round(col("Avg_Price"), 2))


# Write transformed data to HDFS staging path
df_aggregated.write.mode("overwrite").parquet("hdfs://localhost:9000/stg_tbls/")



