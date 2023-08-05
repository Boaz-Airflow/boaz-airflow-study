import airflow.utils.dates
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator


dag = DAG(
    dag_id="listing_6_01",
    start_date=airflow.utils.dates.days_ago(10),
    schedule_interval="@once",
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the FileSensor.",
    default_args={"depends_on_past": True},
)

wait = FileSensor(
    task_id="wait_for_supermarket_1", 
    fs_conn_id='fs_default',
    filepath="supermarket/supermarket1/data.csv", 
    poke_interval=10,
    timeout=20,
    dag=dag
)

dummy_task = BashOperator(
    task_id='cat_a',
    bash_command='cat /opt/airflow/data/supermarket/supermarket1/data.csv',
    dag=dag,
)

wait >> dummy_task