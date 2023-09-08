import time

from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
import boto3
from botocore.exceptions import ClientError

from typing import Sequence, TYPE_CHECKING, Optional

# if TYPE_CHECKING:
#     from airflow.utils.context import Context

class AthenaOperator(BaseOperator):
    
    """
    Custom Airflow Operator for running AWS Athena queries.

    Attributes:
        query (str): The SQL query to execute.
        output_location (str): The S3 location to store query results.
        database (str): The database to run the query on.
        aws_conn_id (str): Airflow AWS connection ID. Default is 'aws_default'.
        region_name (str): AWS region for Athena service. Default is 'ap-northeast-2'.
        max_time (int): Maximum time for query execution in seconds. Default is 1200 seconds.
        sleep_time (int): Time to sleep while polling for query completion. Default is 30 seconds.
    """
    
    template_fields: Sequence[str] = ('query', 'output_location')
    template_ext: str = ".sql"

    @apply_defaults
    def __init__(
        self,
        query,
        output_location,
        database,
        aws_conn_id='aws_default',
        region_name='ap-northeast-2',
        max_time=1200,
        sleep_time=30,
        *args, **kwargs
    ):
        
        """
        Initialize the AthenaOperator.

        Args:
            query (str): The SQL query to execute.
            output_location (str): The S3 location to store query results.
            database (str): The database to run the query on.
            aws_conn_id (str): Airflow AWS connection ID. Default is 'aws_default'.
            region_name (str): AWS region for Athena service. Default is 'ap-northeast-2'.
            max_time (int): Maximum time for query execution in seconds. Default is 1200 seconds.
            sleep_time (int): Time to sleep while polling for query completion. Default is 30 seconds.
        """
        
        super().__init__(*args, **kwargs)
        self.query = query
        self.output_location = output_location
        self.database = database
        self.aws_conn_id = aws_conn_id
        self.sleep_time = sleep_time
        self.region_name = region_name
        self.start_time = time.time()
        self.current_time = None
        self.max_time = max_time
        self.time_out = False
        self.query_execution_id = None  # initialize with None


    def execute(self, context) -> Optional[str]:
        """
        Execute the Athena query.

        Args:
            context (dict): Airflow context parameters.

        Returns:
            Optional[str]: Query execution ID if successful, None otherwise.

        Raises:
            AirflowException: If the query execution failed or timed out.
        """
        aws_hook = AwsHook(self.aws_conn_id)
        creds = aws_hook.get_credentials()

        client = boto3.client(
            'athena',
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            region_name=self.region_name
        )

        response = client.start_query_execution(
            QueryString=self.query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.output_location}
        )

        self.query_execution_id = response['QueryExecutionId']

        # Polling for the query completion
        while True:
            response = client.get_query_execution(QueryExecutionId=self.query_execution_id)
            state = response['QueryExecution']['Status']['State']

            self.current_time = time.time()

            if int(self.current_time - self.start_time) > self.max_time: # If the query execution time exceeds max time
                self.time_out = True
                break


            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            else:
                self.log.info("Query is still running. Sleep for %s seconds", self.sleep_time)
                time.sleep(self.sleep_time)


        if self.time_out:
            self.log.error("Query execution exceeds: %d seconds", self.max_time)
            raise AirflowException(f"Redshift query execution excceds {self.max_time} seconds")
            
        if state == 'FAILED':
            self.log.error("Query execution failed: %s", response['QueryExecution']['Status']['StateChangeReason'])
            raise AirflowException("Athena query failed")
        elif state == 'CANCELLED':
            self.log.warning("Query was cancelled.")
            raise AirflowException("Athena query was cancelled")

        self.log.info("Query completed successfully")
        
        return self.query_execution_id


    def on_kill(self) -> None:
        """
        Cancel the running query when receiving a kill signal.

        Raises:
            ClientError: If unable to cancel the query.
        """        
        client = boto3.client('athena', region_name=self.region_name)  # Use the defined region_name
        if self.query_execution_id:
            self.log.info("Received a kill signal.")
            try:
                client.stop_query_execution(QueryExecutionId=self.query_execution_id)
            except ClientError as e:
                self.log.exception(f"Exception while cancelling query. Query execution id: {self.query_execution_id}. Error: {str(e)}")