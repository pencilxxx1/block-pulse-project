from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Extraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 12),
    'email': ['agboolakeem00@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_on_delay': timedelta(minutes=1)
}

dag = DAG(
    'blockpulse_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for Blockpulse data',
)

# Define the tasks in the DAG
extraction = PythonOperator(
    task_id ='extraction_layer',
    python_callable = run_extraction
    dag=dag,
)

transformation = PythonOperator(
    task_id ='transformation_layer',
    python_callable = run_transformation,
    dag=dag,
)

loading = PythonOperator(
    task_id ='loading_layer',
    python_callable = run_loading,
    dag=dag,
)

# Set the task dependencies
extraction >> transformation >> loading
