from datetime import datetime as dtime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
import time

def get_config_params(**kwargs):
    context = get_current_context()
    print(f"context value: {context}")
    logical_date = kwargs["logical_date"]
    custom_param = kwargs["dag_run"].conf.get("custom_parameter")
    todays_date = dtime.now().date()

    if logical_date.date() == todays_date:
        print("Normal Execution")
    else:
        print("Back-dated Execution")
        if custom_param is not None:
            print(f"Custom Parameter Value is: {custom_param}")
    
    time.sleep(15)  # sleep for 15 seconds

def_args = {
    "owner": "airflow",
    "retries": 0,
    "start_date": dtime(2021, 1, 1)
}

with DAG("dag_config_params", 
         default_args=def_args, 
         catchup=False) as dag:
    
    start = DummyOperator(task_id="START")
    
    config_params = PythonOperator(
        task_id="DAG_CONFIG_PARAMS",
        python_callable=get_config_params
    )
    
    end = DummyOperator(task_id="END")

    start >> config_params >> end