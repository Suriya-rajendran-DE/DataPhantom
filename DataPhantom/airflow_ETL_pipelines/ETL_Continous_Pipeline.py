"""
This ETL is to demonstrate how to run a DAG Continuously
"""
from datetime import datetime as dtime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def transform_fn():
    print("Current Execution DateTime ", dtime.now())
    print("Logic to Transform Data")

def_args = {
    "owner": "airflow",
    "start_date": dtime(2022, 1, 1)
}

with DAG("ETL_Continuous_Pipeline",
         catchup=False,
         schedule="@continuous",
         max_active_runs=1,
         default_args=def_args) as dag:

    start = EmptyOperator(task_id = "START")
    e = EmptyOperator(task_id = "EXTRACT")
    t = PythonOperator(task_id = "TRANSFORM", python_callable = transform_fn)
    l = EmptyOperator(task_id = "LOAD")
    end = EmptyOperator(task_id = "END")

    start >> e >> t >> l >> end