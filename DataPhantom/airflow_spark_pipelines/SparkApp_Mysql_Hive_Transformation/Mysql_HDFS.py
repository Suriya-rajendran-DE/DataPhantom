from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession

def get_mysql_jdbc_config(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    jdbc_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}"
    return {
        "url": jdbc_url,
        "user": conn.login,
        "password": conn.password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

def read_and_write_mysql_to_hdfs(conn_id: str, table_name: str, hdfs_output_path: str):
    spark = SparkSession.builder \
        .appName("MySQL_to_HDFS_ETL") \
        .getOrCreate()  # Omit spark.jars config if JDBC jar is preloaded in $SPARK_HOME/jars

    jdbc_config = get_mysql_jdbc_config(conn_id)

    df_mysql = spark.read.format("jdbc").options(
        url=jdbc_config["url"],
        dbtable=table_name,
        user=jdbc_config["user"],
        password=jdbc_config["password"],
        driver=jdbc_config["driver"]
    ).load()

    print(f"Retrieved {df_mysql.count()} records from {table_name}")
    df_mysql.show(1, truncate=False)

    df_mysql.write.mode("overwrite").parquet("hdfs:///data/landing/retail_sales_raw")
    print(f"Data written to HDFS path: {"hdfs:///data/landing/retail_sales_raw"}")