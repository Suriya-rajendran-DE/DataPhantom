
from Task_groups_dag.merge_task import merge_all

def transform_step():
    
    df = merge_all()

    df_cleaned = df.fillna("unknown").withColumnRenamed("txn_date", "transaction_date")
    df_cleaned.createOrReplaceTempView("transformed_data")
    return df_cleaned