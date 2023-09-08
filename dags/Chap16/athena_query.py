# athena_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from aws_athena_operator import AthenaOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain

import time
import os

# Constants
DATABASE = 'jd_database'
OUTPUT_LOCATION = 's3://hajun-bucket/athena/'
AWS_CONN_ID = 'aws_default'

BASE_SQL_DIR = os.environ.get('AIRFLOW_VAR_DAGS_SQL_DIR', '/opt/airflow/dags/sqls')
AIRFLOW_SQL_DIR = f'{BASE_SQL_DIR}/athena'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# template search path 
dag = DAG(
    'ch16_athena_query_dag',
    default_args=default_args,
    description='A simple DAG to run Athena queries',
    schedule_interval='@once',
    start_date=datetime(2023, 8, 30),
    # template_searchpath=AIRFLOW_SQL_DIR,
    catchup=False
)

# Start and end tasks for better visualization
start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)

def _short_delay() -> None:
    time.sleep(10)


def _read_sql_file(file_path:str) -> str:
    with open(file_path, 'r') as f:
        return f.read()

dimension_drop_tasks = []
dimension_create_tasks = []


def process_table_queries(create_table_list:list) -> None:
    
    for table_name in create_table_list:
        drop_query = f'DROP TABLE IF EXISTS {DATABASE}.{table_name};'

        drop_task = AthenaOperator(
            task_id=f'run_athena_drop_{table_name}',
            query=drop_query,
            output_location=OUTPUT_LOCATION,
            database=DATABASE,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

        delay_task = PythonOperator(
            task_id=f'short_delay_before_create_{table_name}',
            python_callable=_short_delay,
            dag=dag
        )
        
        create_query = _read_sql_file(f'{AIRFLOW_SQL_DIR}/create_{table_name}.sql')
        
        create_task = AthenaOperator(
            task_id=f'run_athena_create_{table_name}',
            query=create_query,
            output_location=OUTPUT_LOCATION,
            database=DATABASE,
            aws_conn_id=AWS_CONN_ID,
            dag=dag
        )

        drop_task >> delay_task >> create_task
        
        dimension_drop_tasks.append(drop_task)
        dimension_create_tasks.append(create_task)


create_table_list = [
    'jd_count', 'jd_date'
    
]

process_table_queries(create_table_list=create_table_list)

# Setting up the dependencies for start and end tasks
start_task >> dimension_drop_tasks
dimension_create_tasks >> end_task


