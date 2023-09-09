import airflow
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from dateutil.parser import parse
from lib import Environment, _send_discord_message

ENV = Environment("CT")


def _generate_queries(ts):
    ts = parse(ts)
    return f"""
            SELECT * FROM continuous_training
            WHERE time >= TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S%z')}' - INTERVAL '2 hours'
            AND time <= TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S%z')}';
            """


@dag(
    dag_id="Continuous-Training",
    start_date=airflow.utils.dates.days_ago(2),
    end_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
    catchup=True,
    tags=["MLOps", "Continuous Training"],
)
def continuous_training():
    generate_queries = PythonOperator(
        task_id="generate_queries", python_callable=_generate_queries
    )

    send_training_log = PythonOperator(
        task_id="send_training_log",
        python_callable=_send_discord_message,
        op_kwargs={
            "webhook_url": ENV.WEBHOOK,
            "content": ":computer: [{{ ts }}]: TRAINING START!",
        },
    )

    training = KubernetesPodOperator(
        task_id="training",
        name="training",
        image="zerohertzkr/airflow-continuous-training",
        env_vars={
            "WEBHOOK": ENV.WEBHOOK,
            "CLASSES": str(ENV.CLASSES),
            "TIME": "{{ ts }}",
            "QUERY": "{{ ti.xcom_pull(task_ids='generate_queries', key='return_value') }}",
            "user": ENV.DB_USER,
            "password": ENV.DB_PASSWORD,
            "host": ENV.DB,
            "port": ENV.DB_PORT,
            "database": ENV.DB_DATABASE,
        },
    )

    generate_queries >> [send_training_log, training]


DAG = continuous_training()
