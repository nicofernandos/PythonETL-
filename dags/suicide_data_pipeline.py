from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'farhan',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kaggle_suicide_pipeline',
    default_args=default_args,
    description='Pipeline ETL Suicide Data dari CSV ke Postgres',
    schedule_interval='@daily', 
    catchup=False
)

run_etl = BashOperator(
    task_id='run_pyspark_transform',
    bash_command='python3 "/media/farhan/DATA PRIBADI/Project/pipeline/scripts/transform.py"',
    dag=dag,
)

run_etl
