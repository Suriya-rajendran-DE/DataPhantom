from Task_groups_dag.read_tables import read_table1, read_table2, read_table3

def merge_all():
    df1 = read_table1()
    df2 = read_table2()
    df3 = read_table3()

    df_merged = df1.join(df2, "customer_id").join(df3, "customer_id")
    df_merged.createOrReplaceTempView("merged_customers")
    return df_merged