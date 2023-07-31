from urllib import request

import airflow
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def _get_data(year, month, day, hour, output_path):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


def _fetch_pageviews(pagenames, execution_date):
    result = dict.fromkeys(pagenames, 0)
    with open("/opt/airflow/data/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    with open("/opt/airflow/data/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO boaz VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )
    """
    NOTE: Write SQL query
    """


@dag(
    dag_id="Chap04_6",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/opt/airflow/data",
    max_active_runs=1,
)
def Chap04():
    init_data = BashOperator(
        task_id="init_data",
        bash_command="rm -rf /opt/airflow/data/wikipageviews.gz",
    )

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

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="gunzip --force /opt/airflow/data/wikipageviews.gz",
    )

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "pagenames": {
                "Google",
                "Amazon",
                "Apple",
                "Microsoft",
                "Facebook",
            },
        },
    )

    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        postgres_conn_id="postgres-server",
        sql="postgres_query.sql",
    )
    """
    NOTE: Write to postgresql
    """

    init_data >> get_data >> extract_gz >> fetch_pageviews >> write_to_postgres


DAG = Chap04()
