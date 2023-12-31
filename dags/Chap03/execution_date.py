import json
import pathlib
import datetime as dt

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="boaz_execution",
    description='A simple tutorial DAG',
    schedule_interval= '1 * * * *', # 매 시간 1분 실행
    start_date=dt.datetime(2023,7,20)
)

fetch_events = BashOperator(
    task_id="bash_task",
    bash_command='echo "Hi from bash operator : {{ds}}"',
    dag =dag
)

def _basic_python():
    print("hi boaz")

basic_operation = PythonOperator(
    task_id="python_task",
    python_callable=_basic_python,
    dag = dag
)

fetch_events >> basic_operation