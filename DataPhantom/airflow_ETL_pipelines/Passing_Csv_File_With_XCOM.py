from datetime import datetime as dtime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import csv  # Needed for csv reading

# File path using WSL mount
CSV_PATH = "/mnt/d/CM_datasets/emp.csv"

def extract_fn(**kwargs):
    ti = kwargs['ti']
    print("Extracting data from:", CSV_PATH)
    
    ti.xcom_push(key='data_path', value=CSV_PATH)
    print("Data path pushed to XCom.")

def transform_fn(**kwargs):
    ti = kwargs['ti']
    path = ti.xcom_pull(task_ids='extract', key='data_path')
    print("Transform step. Reading from:", path)

    with open(path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        row_count = sum(1 for row in reader) - 1  # exclude header
    print(f"Number of data rows (excluding header): {row_count}")

def load_fn(**kwargs):
    ti = kwargs['ti']
    path = ti.xcom_pull(task_ids='extract', key='data_path')
    print("Load step. Reading from:", path)

    with open(path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    if rows:
        total_rows = len(rows) - 1  # exclude header
        total_columns = len(rows[0])
        print(f"Total Rows (excluding header): {total_rows}")
        print(f"Total Columns: {total_columns}")
    else:
        print("CSV file is empty!")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dtime(2025, 6, 1)
}
"""
default_args = {
    'owner': 'suriya',
    'start_date': dtime(2025, 7, 1),
    'email': ['dataops@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'retry_exponential_backoff': True
}

"""

with DAG(
    dag_id="Passing_Csv_File_With_XCOM",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False
) as dag:

    start = DummyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_fn,
        provide_context=True  # allows your task function to access useful metadata like ti, ds, ts etc.

    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_fn,
        provide_context=True

    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_fn,
        provide_context=True

    )

    end = DummyOperator(task_id="end")

    start >> extract >> transform >> load >> end