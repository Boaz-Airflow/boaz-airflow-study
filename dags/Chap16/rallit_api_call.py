from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from long_http_operator import CustomSimpleHttpOperator

import json
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
from datetime import date, timedelta

def _handle_response(response):
    logging.info(response.text)
    if response.status_code == 200:
        logging.info(response.json())
    else:
        logging.error(response.text)
        raise ValueError("API call failed: " + response.text)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_tasks': 7,
}

with DAG(
    'ch16_rallit_job_api_scraper_dag',
    default_args=default_args,
    description='A async user rallit_job_scraper_api call dag',
    start_date=datetime(2023, 8, 30),
    schedule_interval='@once',
) as dag:
    
    task_http_sensor_check = HttpSensor(
        task_id="http_sensor_check",
        http_conn_id="jd_scraper_api",
        endpoint="",
        request_params={},
        response_check=lambda response: "FastAPI" in response.text,
        poke_interval=5,
        timeout=20,
    dag=dag,
    )   
    
    task_get_op = CustomSimpleHttpOperator(
        task_id="get_rallit_job_api",
        # host.docker.internal
        http_conn_id="jd_scraper_api",
        endpoint="/api/v1/scrape-rallit",
        method = "GET",
        headers={"Content-Type": "application/json"},
        # timeout=300, --> timout --> baseoperator 상속 받아야 한다
        response_check=lambda response: _handle_response(response),
    dag=dag,
    )
    
    task_http_sensor_check >> task_get_op
    

    
    
