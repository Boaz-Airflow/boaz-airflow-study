import pytest
import os
from collections import namedtuple
import sys
sys.path.append("/opt/airflow/dags")

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import DAG, Connection
from pytest_docker_tools import fetch, container

from airflowbook.operators.movielens_operator import (
    MovielensHook,
    MovielensToPostgresOperator,
)
@pytest.fixture(scope="module")
def postgres_credentials():
    PostgresCredentials = namedtuple(
        "PostgresCredentials", ["username", "password"])
    return PostgresCredentials("testuser", "testpass")


postgres_image = fetch(repository="postgres:11.1-alpine")
postgres = container(
    image="{postgres_image.id}",
    environment={
        "POSTGRES_USER": "{postgres_credentials.username}",
        "POSTGRES_PASSWORD": "{postgres_credentials.password}",
    },
    ports={"5432/tcp": None},  # 컨테이너의 5432 포트를 호스트의 임의의 열린 포트(None)으로 매핑
    volumes={
        os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
        }
    },
)


def test_movielens_to_postgres_operator(
    mocker, test_dag: DAG, postgres, postgres_credentials
):
    mocker.patch.object(
        MovielensHook,
        "get_connection",
        return_value=Connection(
            conn_id="test", login="airflow", password="airflow"),
    )
    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            conn_id="postgres",
            conn_type="postgres",
            host="localhost",
            login=postgres_credentials.username,
            password=postgres_credentials.password,
            port=postgres.ports["5432/tcp"][0],  # 호스트에 할당된 포트
        ),
    )

    task = MovielensToPostgresOperator(
        task_id="test",
        movielens_conn_id="movielens_id",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        postgres_conn_id="postgres_id",
        insert_query=(
            "INSERT INTO movielens (movieId,rating,ratingTimestamp,userId,scrapeTime) "
            "VALUES ({0}, '{{ macros.datetime.now() }}')"
        ),
        dag=test_dag,
    )

    pg_hook = PostgresHook()

    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count == 0

    pytest.helpers.run_airflow_task(task, test_dag)

    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count > 0
