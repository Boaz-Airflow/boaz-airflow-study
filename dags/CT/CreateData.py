import math

import airflow
import numpy as np
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dateutil.parser import parse
from lib import Environment

ENV = Environment("CT")


def _mean(time, class_name):
    """
    NOTE: 시간과 Class에 따른 평균 X, Y 조회
    SELECT
    time,
    class,
    AVG(x) AS avg_x,
    AVG(y) AS avg_y
    FROM
        continuous_training
    WHERE
        class IN ('A', 'B', 'C')
    GROUP BY
        time, class
    ORDER BY
        time, class;
    """
    idx = ENV.CLASSES.index(class_name)
    time = time / 24 * 2 * math.pi
    mean_x = math.sqrt(ENV.RADIUS) * math.cos(
        time + 2 * math.pi * idx / len(ENV.CLASSES)
    )
    mean_y = math.sqrt(ENV.RADIUS) * math.sin(
        time + 2 * math.pi * idx / len(ENV.CLASSES)
    )
    return mean_x, mean_y


def _generate_queries(class_name, num_entries, ts):
    queries = []
    mean_x, mean_y = _mean(int(ts[11:13]), class_name)
    ts = parse(ts)
    for _ in range(num_entries):
        x = np.random.normal(mean_x, 1)
        y = np.random.normal(mean_y, 1)
        queries.append(
            f"INSERT INTO CONTINUOUS_TRAINING (time, x, y, class) VALUES ('{ts.strftime('%Y-%m-%d %H:%M:%S%z')}', {x}, {y}, '{class_name}');"
        )
    return "\n".join(queries)


def _merge_queries(ti):
    queries = []
    for c in ENV.CLASSES:
        queries.append(ti.xcom_pull(task_ids=f"generate_data_{c}"))
    return "\n".join(queries)


@dag(
    dag_id="Create-Data",
    start_date=airflow.utils.dates.days_ago(2),
    end_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=12,
    catchup=True,
    tags=["MLOps", "Data Engineering"],
)
def create_data():
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=ENV.DB,
        sql="""
        CREATE TABLE IF NOT EXISTS
        CONTINUOUS_TRAINING (
            time TIMESTAMP NOT NULL,
            x FLOAT NOT NULL,
            y FLOAT NOT NULL,
            class TEXT NOT NULL
        );
        """,
    )

    generate_queries = []

    for c in ENV.CLASSES:
        generate_query = PythonOperator(
            task_id=f"generate_data_{c}",
            python_callable=_generate_queries,
            op_args=[c, ENV.NO_DATA],
            do_xcom_push=True,
        )
        generate_queries.append(generate_query)

    merge_queries = PythonOperator(
        task_id="merge_queries", python_callable=_merge_queries, do_xcom_push=True,
    )

    push_data = PostgresOperator(
        task_id="push_data",
        postgres_conn_id=ENV.DB,
        sql="{{ ti.xcom_pull(task_ids='merge_queries', key='return_value') }}",
    )

    create_table >> generate_queries >> merge_queries >> push_data


DAG = create_data()
