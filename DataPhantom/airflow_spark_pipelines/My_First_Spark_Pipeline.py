from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='spark_submit_test_local',
    default_args=default_args,
    schedule_interval=None,
    description='Run Spark job on local[*] via SparkSubmitOperator',
    tags=['spark', 'local']
) as dag:

    run_spark = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/mnt/d/Apache_spark/scripts/my_first_spark_job.py',
        conn_id='spark_default_local',
        application_args=[],
        conf={'spark.master': 'local[*]'},
        verbose=True
    )