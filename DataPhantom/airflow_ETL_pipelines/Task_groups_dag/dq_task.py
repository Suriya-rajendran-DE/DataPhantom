
from Task_groups_dag.transform_task import transform_step


def dq_step():
    
    df = transform_step()

    null_counts = df.select([df[col].isNull().cast("int").alias(col) for col in df.columns]).groupBy().sum()
    null_counts.show()

    # Example DQ check: amount > 0
    df.filter("amount <= 0 OR amount IS NULL").show()