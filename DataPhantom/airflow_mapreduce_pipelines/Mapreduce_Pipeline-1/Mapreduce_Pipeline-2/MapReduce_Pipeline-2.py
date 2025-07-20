from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='local_wordcount_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Local PySpark WordCount using Airflow',
) as dag:

    start = EmptyOperator(task_id='start')

    run_wordcount_local = SparkSubmitOperator(
        task_id='run_wordcount_local',
        application='/mnt/d/Airflow_Dags_Scripts/Maprteduce_Dags/Mapreduce_Pipeline-2/wordcount.py',  # ✅ Updated script path
        conn_id='spark_default_local',                           # ✅ Matches your local Spark conn
        application_args=[
            '/mnt/d/Airflow_Dags_Scripts/Maprteduce_Dags/Datasets/input/mapred-2.txt', 
            '/mnt/d/Airflow_Dags_Scripts/Maprteduce_Dags/Datasets/output/mapred-2'
        ],
        conf={
            'spark.master': 'local[*]'
        },
        verbose=True
    )

    end = EmptyOperator(task_id='end')

    start >> run_wordcount_local >> end