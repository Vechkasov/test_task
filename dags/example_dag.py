from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

def print_hello():
    return "Hello Airflow!"

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
    'example_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['example']
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    bash_task = BashOperator(
        task_id='bash_hello',
        bash_command='echo "Running bash command" && date'
    )

    libraries_check = BashOperator(
        task_id='libraries_check',
        bash_command='pip list'
    )

    python_task = PythonOperator(
        task_id='python_hello',
        python_callable=print_hello
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> bash_task >> libraries_check >> python_task >> end
