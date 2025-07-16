from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='Python_Mapreduce_Job',
    default_args=default_args,
    description='Run a MapReduce job using Python scripts via Hadoop Streaming',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mapreduce', 'python', 'hadoop'],
) as dag:

    cleanup_output = BashOperator(
        task_id='cleanup_output',
        bash_command='hdfs dfs -rm -r -f /MapReduce/output || true',
    )

    run_mapreduce = BashOperator(
        task_id='run_python_streaming_job',
        bash_command="""
        hadoop jar /mnt/c/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
        -input /MapReduce/input \
        -output /MapReduce/output \
        -mapper /mnt/d/Airflow_Dags_Scripts/Maprteduce_Dags/Mapreduce_Pipeline-1/Mapper.py \
        -reducer /mnt/d/Airflow_Dags_Scripts/Maprteduce_Dags/Mapreduce_Pipeline-1/Reducer.py \
        -file /mnt/d/Airflow_Dags_Scripts/Maprteduce_Dags/Mapreduce_Pipeline-1/Mapper.py \
        -file /mnt/d/Airflow_Dags_Scripts/Maprteduce_Dags/Mapreduce_Pipeline-1/Reducer.py
        """
    )

    cleanup_output >> run_mapreduce