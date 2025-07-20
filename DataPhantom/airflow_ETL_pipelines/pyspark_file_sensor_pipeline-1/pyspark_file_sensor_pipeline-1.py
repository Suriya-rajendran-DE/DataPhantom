from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

CSV_DIR = "/mnt/d/Airflow_Dags_Scripts/FILE_SNSR_DAGS/file_sensor/file_sensor-1/input"

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_file_sensor_pipeline-1',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description='ETL pipeline triggered by arrival of any CSV file in a folder',
) as dag:

    start = EmptyOperator(task_id='start')

    wait_for_csv = FileSensor(
        task_id='wait_for_csv',
        filepath=os.path.join(CSV_DIR, "*.csv"),
        poke_interval=10,
        timeout=600,
        mode='poke',
        fs_conn_id='fs_default',  # Make sure this connection points to the correct base dir
    )

    run_spark_etl = SparkSubmitOperator(
        task_id='run_spark_etl',
        application='/mnt/d/Airflow_Dags_Scripts/FILE_SNSR_DAGS/pyspark_file_sensor_pipeline-1/spark_etl_logic_file_sensor-1.py',
        conn_id='spark_default_local',
        application_args=[CSV_DIR],
        conf={"spark.master": "local[*]"},
        verbose=True,
    )

    end = EmptyOperator(task_id='end')

    start >> wait_for_csv >> run_spark_etl >> end