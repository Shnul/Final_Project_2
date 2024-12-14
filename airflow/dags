from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pyspark_dag',
    default_args=default_args,
    description='A simple DAG to run multiple PySpark scripts',
    schedule=timedelta(days=1),
)

def print_success():
    print("Task succeeded!")

def print_failure():
    print("Task failed!")

task1 = BashOperator(
    task_id='kafka_to_postgres',
    bash_command='spark-submit /path/to/your/pyspark_script_1.py',
    dag=dag,
)

task2 = BashOperator(
    task_id='API_exchange_rate_topic',
    bash_command='spark-submit /path/to/your/pyspark_script_2.py',
    dag=dag,
)

success_task = PythonOperator(
    task_id='print_success',
    python_callable=print_success,
    dag=dag,
)

failure_task = PythonOperator(
    task_id='print_failure',
    python_callable=print_failure,
    trigger_rule='one_failed',
    dag=dag,
)

task1 >> task2 >> success_task
[task1, task2] >> failure_task
