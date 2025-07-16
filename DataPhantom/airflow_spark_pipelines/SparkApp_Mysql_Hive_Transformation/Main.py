from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
    'start_date': datetime(2024, 5, 16),
}

# Define base path to your DAG scripts (update if your actual path is different)
BASE_PATH = "/home/suriya/airflow_pjt/dags/DB_DAGS/MYSQL_HIVE"

def run_script(script_name):
    script_path = os.path.join(BASE_PATH, script_name)
    print(f"Running script: {script_path}")

    try:
        result = subprocess.run(
            ["python3", script_path],
            check=True,
            capture_output=True,
            text=True
        )
        # Log successful output
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)

    except subprocess.CalledProcessError as e:
        # Log both stdout + stderr from failed run
        print(f"Error while executing {script_name}")
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)

        # Raise error to make Airflow mark this task as failed
        raise RuntimeError(
            f"Script {script_name} failed. See logs above for details."
        ) from e
        
        
        

with DAG(
    dag_id="spark_etl_pipeline_Mysql_Hive",
    default_args=default_args,
    description="MySQL to Hive ETL Pipeline using Spark",
    schedule_interval=None,
    catchup=False
) as dag:

    start = EmptyOperator(task_id="START")

    task_1 = PythonOperator(
        task_id="MySQL_HDFS_EXTRACTION",
        python_callable=lambda: run_script("Mysql_HDFS.py")
    )

    task_2 = PythonOperator(
        task_id="SPARK_HIVE_TBLS_CREATION",
        python_callable=lambda: run_script("Spark_Hive_Tbl.py")
    )

    task_3 = PythonOperator(
        task_id="SPARK_DATA_TRANSFORMATION",
        python_callable=lambda: run_script("Transformation.py")
    )

    task_4 = PythonOperator(
        task_id="SPARK_DQ_CHECKS",
        python_callable=lambda: run_script("DQ_Checks.py")
    )

    task_5 = PythonOperator(
        task_id="ARCHIVED",
        python_callable=lambda: run_script("Archive_Raw_Tbl.py")
    )

    end = EmptyOperator(task_id="END")

    # Define task dependencies
    start >> task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> end