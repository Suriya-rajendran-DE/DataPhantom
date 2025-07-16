
"""
Set Variables in CLI
airflow variables set greeting_name "Suriya"
airflow variables set greeting_message "You're doing great!"


Or head to the UI → Admin → Variables.

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def print_greeting():
    name = Variable.get("greeting_name", default_var="stranger")
    message = Variable.get("greeting_message", default_var="Welcome to Airflow!")
    print(f"{message}, {name}!")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="variable_example_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["variable", "demo"]
) as dag:

    greet_task = PythonOperator(
        task_id="print_variable_greeting",
        python_callable=print_greeting
    )