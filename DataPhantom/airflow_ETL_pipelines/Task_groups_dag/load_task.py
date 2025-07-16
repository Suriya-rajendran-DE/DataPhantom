from Task_groups_dag.transform_task import transform_step
from Task_groups_dag.db_conn import get_mysql_jdbc_config

def load_step():
    df = transform_step()

    # Get row and column stats before writing
    row_count = df.count()
    column_count = len(df.columns)

    # JDBC config for write
    config = get_mysql_jdbc_config("MySql")

    # Write to MySQL
    df.write.format("jdbc").options(
        url=config["url"],
        dbtable="final_transformed_customers",
        user=config["user"],
        password=config["password"],
        driver=config["driver"]
    ).mode("overwrite").save()

    # Confirmation log
    print("Data successfully written to final_transformed_table")
    print(f"Loaded {row_count} rows and {column_count} columns.")