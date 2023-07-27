from urllib import request

import airflow
from airflow.decorators import dag
from airflow.operators.python import PythonOperator


def _get_data(year, month, day, hour, output_path):
    """
    NOTE: op_kwargs
    """
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


def _print_context(**kwargs):
    print("=" * 100)
    for i, j in kwargs.items():
        print(i, ":\t", j)
    print("=" * 100)


@dag(
    dag_id="Chap04_4",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
)
def Chap04():
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
            "year": "{{ execution_date.year }}",
            "month": "{{ execution_date.month }}",
            "day": "{{ execution_date.day }}",
            "hour": "{{ execution_date.hour }}",
            "output_path": "/opt/airflow/data/wikipageviews.gz",
        },
    )
    """
    NOTE: op_kwargs
    {
        "day": "24",
        "hour": "1",
        "month": "7",
        "output_path": "/opt/airflow/data/wikipageviews.gz",
        "year": "2023"
    }
    """

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )

    get_data >> print_context


DAG = Chap04()
