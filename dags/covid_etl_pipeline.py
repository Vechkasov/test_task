from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['admin@example.com'],
    'email_on_failure': False,   # Отключаем email до настройки SMTP
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='covid_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for covid data processing',
    schedule_interval="@daily",
) as dag:
    
     # Общая конфигурация Spark
    spark_conf = {
        'spark.master': 'local[*]',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.jars': '/opt/spark/jars/postgresql-42.7.0.jar',
        'spark.driver.bindAddress': '0.0.0.0',
        'spark.driver.host': 'localhost',
        'spark.driver.port': '0',
        'spark.driver.blockManager.port': '0',
        'spark.network.timeout': '600s',
        'spark.executor.heartbeatInterval': '60s',
        'spark.sql.warehouse.dir': 'file:///tmp/spark-warehouse',
        'spark.sql.adaptive.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
    }

    task_load_raw = SparkSubmitOperator(
        task_id='load_raw',
        application='/usr/local/airflow/dags/load_raw.py',
        conn_id='spark_default',
        conf=spark_conf,
        queue=None,
        verbose=True
    )

    task_transform_stage = SparkSubmitOperator(
        task_id='transform_stage',
        application='/usr/local/airflow/dags/transform_stage.py',
        conn_id='spark_default',
        conf=spark_conf,
        verbose=True
    )

    task_create_mart = SparkSubmitOperator(
        task_id='create_mart',
        application='/usr/local/airflow/dags/create_mart.py',
        conn_id='spark_default',
        conf=spark_conf,
        verbose=True
    )

    task_load_raw >> task_transform_stage >> task_create_mart