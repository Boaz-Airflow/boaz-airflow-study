import airflow
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from lib import Environment

ENV = Environment("CT")


@dag(
    dag_id="Reset-DB",
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval="@once",
    max_active_runs=1,
    catchup=False,
    tags=["MLOps", "Data Engineering"],
)
def reset_db():
    reset = PostgresOperator(
        task_id="reset_db",
        postgres_conn_id=ENV.DB,
        sql="DROP TABLE continuous_training;",
    )

    reset


DAG = reset_db()
