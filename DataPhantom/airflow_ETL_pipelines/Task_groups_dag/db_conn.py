from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook

def get_mysql_jdbc_config(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    jdbc_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}"
    return {
        "url": jdbc_url,
        "user": conn.login,
        "password": conn.password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "target_schema": conn.schema  # original schema to filter
    }

def list_tables_in_schema(conn_id: str):
    spark = SparkSession.builder.appName("MySQL_List_Tables").getOrCreate()
    config = get_mysql_jdbc_config(conn_id)

    df_tables = spark.read.format("jdbc").options(
        url=config["url"],
        dbtable="TABLES",
        user=config["user"],
        password=config["password"],
        driver=config["driver"]
    ).load()

    # Filter for your schema only
    filtered = df_tables.filter(df_tables["TABLE_SCHEMA"] == config["target_schema"])
    table_names = filtered.select("TABLE_NAME").collect()

    print(f"Found {len(table_names)} tables in schema '{config['target_schema']}':")
    for row in table_names:
        print(f"  - {row['TABLE_NAME']}")

    return table_names