from datetime import datetime
import time
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False, # True로 설정하면, start_date 이전의 DAG도 실행함
    'retries': 1,
}

def _get_airflow_variable_or_default(variable_name, default_value=None):
    try:
        return Variable.get(variable_name)
    except KeyError:
        return default_value

with DAG('ch16_glue_crawler_dag', 
         description='Run Glue Crawler',
         schedule_interval=None,
         start_date=datetime(2023, 8, 30),
         default_args=DEFAULT_ARGS) as dag:
    
    @task()
    def start_crawler(crawler_name:str) -> str:
        access_key = _get_airflow_variable_or_default("access_key")
        secret_key = _get_airflow_variable_or_default("secret_key")
        region_name = _get_airflow_variable_or_default("region_name")
        glue_client = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        glue_client.start_crawler(Name=crawler_name)
        
        message = f"Start crawler: {crawler_name}"
        print(f"Start crawler: {crawler_name}")
        
        return message

        
    @task()
    def check_crawler_status(crawler_name:str) -> str:
        '''
        avoid READY, RUNNING, STOPPING status
        '''
        
        access_key = _get_airflow_variable_or_default("access_key")
        secret_key = _get_airflow_variable_or_default("secret_key")
        region_name = _get_airflow_variable_or_default("region_name")
        glue_client = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        response = glue_client.get_crawler(Name=crawler_name)['Crawler']['State']
        
        if response == 'READY':
            raise Exception("Crawler is in READY state. Please check the crawler.")
        else:
            while response in ['RUNNING', 'STOPPING']:
                response = glue_client.get_crawler(
                    Name=crawler_name
                    )['Crawler']['State']
                print(f"Current status: {response}")
                time.sleep(4)
            
            else:
                print(f"Current status: {response}")
                return f"Current status: {response}" 
    
    @task()
    def log_final_message() -> None:
        print("All crawlers have completed successfully!")
    
    
    crawler_name = 'jd_crawler' # crawler name
    start_crawler(crawler_name=crawler_name) >> check_crawler_status(crawler_name=crawler_name) >> log_final_message()


    
  