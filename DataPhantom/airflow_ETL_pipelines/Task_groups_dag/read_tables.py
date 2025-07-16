from pyspark.sql import SparkSession
from Task_groups_dag.db_conn import get_mysql_jdbc_config

def read_table1():
    return _read_table("MySql", "customer_profiles")

def read_table2():
    return _read_table("MySql", "customer_transactions")

def read_table3():
    return _read_table("MySql", "customer_locations")

def _read_table(conn_id, table_name):
    spark = SparkSession.builder.appName("Task_group_app").getOrCreate()
    config = get_mysql_jdbc_config(conn_id)

    return spark.read.format("jdbc").options(
        url=config["url"],
        dbtable=table_name,
        user=config["user"],
        password=config["password"],
        driver=config["driver"]
    ).load()