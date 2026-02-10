from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import pandas as pd

def read_csv():
    df = pd.read_csv('/usr/local/airflow/data/raw_2020.csv', sep=';')
    print('\n' + df.head().to_string())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_read_csv',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    ls_data = BashOperator(
        task_id='ls_data',
        bash_command='ls -l /usr/local/airflow/data'
    )

    pandas_read = PythonOperator(
        task_id='pandas_read',
        python_callable=read_csv
    )

    pyspark_read = SparkSubmitOperator(
        task_id='spark_read_csv',
        application='/usr/local/airflow/dags/spark_scripts/example_read_csv.py',
        conn_id='spark_default',
        verbose=True,
        application_args=[
            '--input_path', '/usr/local/airflow/data/raw_2020.csv',
            '--separator', ';',
            '--limit', '10'
        ],
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        num_executors=1,
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> ls_data >> [pandas_read, pyspark_read] >> end
