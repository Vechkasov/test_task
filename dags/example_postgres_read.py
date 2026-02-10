from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

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
    'example_postgres_read',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    start = DummyOperator(task_id='start')
    
    read_postgres = SparkSubmitOperator(
        task_id='read_postgres',
        application='/usr/local/airflow/dags/spark_scripts/example_read_postgres.py',
        conn_id='spark_default',
        verbose=True,
        application_args=[
            '--table', 'ab_permission',
            '--limit', '5'
        ],
        jars='/opt/spark/jars/postgresql-42.7.0.jar',
        driver_class_path='/opt/spark/jars/postgresql-42.7.0.jar',
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        num_executors=1,
    )
    
    finish = DummyOperator(task_id='finish')
    
    start >> read_postgres >> finish