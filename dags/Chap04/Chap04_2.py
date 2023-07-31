from urllib import request

import airflow
from airflow.decorators import dag
from airflow.operators.python import PythonOperator


def _get_data(execution_date):
    """
    NOTE: Template of PythonOperator
    """
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/opt/airflow/data/wikipageviews.gz"
    request.urlretrieve(url, output_path)


def _print_context(**kwargs):
    print("=" * 100)
    for i, j in kwargs.items():
        print(i, ":\t", j)
    print("=" * 100)


@dag(
    dag_id="Chap04_2",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
)
def Chap04():
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
    )

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )

    get_data >> print_context


DAG = Chap04()
