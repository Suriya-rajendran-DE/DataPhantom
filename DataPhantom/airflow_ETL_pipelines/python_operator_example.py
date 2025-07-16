from datetime import datetime as dtime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


def extract_fn():
    print(" Data has been extracted successfully")
    
def transform_fn(a1):
    print("Transformed and", a1)

def load_fn(p1, p2):
    print("Transformed data has been {}".format(p1))
    print("Data has been {}".format(p2))
    print("Data is ready for bizz insights")
    
def_args = {
    "owner" : "suriya",
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1),
    "start_date": dtime(2025 ,5, 10)
}

with DAG ("Ex_Python_operator_Dag",
    default_args = def_args, 
    catchup = False) as dag:
    
    start = DummyOperator(task_id = "START")

    e = PythonOperator(
    task_id = "EXTRACT",
    python_callable = extract_fn
    )

    t = PythonOperator(
    task_id = "TRANSFORM",
    python_callable = transform_fn,
    op_args = ["cleaned data for analysis"]
    )

    l = PythonOperator(
    task_id = "LOAD",
    python_callable = load_fn,
    op_args = ["loaded", "loaded in the target table succesfully"]
    )

    end = DummyOperator(task_id = "END")

start >> e >> t >> l >> end

