import airflow
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _print_context(**kwargs):
    print("=" * 100)
    for i, j in kwargs.items():
        print(i, ":\t", j)
    print("=" * 100)


@dag(
    dag_id="Chap04_1",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
)
def Chap04():
    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "curl -o /opt/airflow/data/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/"
            "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year }}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        ),
    )
    """
    NOTE: execution_date를 통한 API 호출
    """

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )
    """
    NOTE: Task Context 출력
    """

    get_data >> print_context


DAG = Chap04()
