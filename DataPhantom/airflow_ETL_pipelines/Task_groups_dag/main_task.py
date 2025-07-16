from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
from Task_groups_dag.read_tables import read_table1, read_table2, read_table3
from Task_groups_dag.merge_task import merge_all
from Task_groups_dag.transform_task import transform_step
from Task_groups_dag.dq_task import dq_step
from Task_groups_dag.load_task import load_step



default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}



with DAG("Task_Groups_dag", 
         default_args=default_args, 
         schedule_interval=None, 
         catchup=False, 
         tags=["interview", "spark", "mysql", "etl"]) as dag:
    

    start = DummyOperator(task_id="START")
    end = DummyOperator(task_id="END")

    with TaskGroup("read_group", tooltip="Read 3 MySQL Tables in Parallel") as read_group:
        t1 = PythonOperator(task_id="read_table1", python_callable=read_table1)
        t2 = PythonOperator(task_id="read_table2", python_callable=read_table2)
        t3 = PythonOperator(task_id="read_table3", python_callable=read_table3)

    merge = PythonOperator(task_id="merge_tables", python_callable=merge_all)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_step)
    dq = PythonOperator(task_id="dq_checks", python_callable=dq_step)
    load = PythonOperator(task_id="load_data", python_callable=load_step)
    
    

    # Define flow
    start >> read_group >> merge >> transform >> dq >> load >> end