import json
from datetime import timedelta

import requests
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


class CustomPostgresOperator(PostgresOperator):
    """
    Custom Postgres operator that ensures the ``sql`` field is templated.
    """

    template_fields = ("sql",)


def get_user(**context):
    """
    Fetch user data from the ReqRes API using an HttpHook.
    The JSON response is returned and pushed to XCom.
    """
    http = HttpHook(http_conn_id="reqres", method="GET")
    response = http.run(endpoint="api/users?page=1")
    response.raise_for_status()
    return response.json()


def process_user(**context):
    """
    Parse the JSON payload from ``get_user`` and extract first name,
    last name, and email for each user. Returns a list of dictionaries
    that will be used by downstream tasks via XCom.
    """
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="get_user")
    users = data.get("data", [])
    processed = [
        {
            "firstname": user.get("first_name", ""),
            "lastname": user.get("last_name", ""),
            "email": user.get("email", ""),
        }
        for user in users
    ]
    return processed


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_data_pipeline",
    default_args=default_args,
    description="Fetch, process, and store user data from ReqRes API",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
) as dag:
    get_user_task = PythonOperator(
        task_id="get_user",
        python_callable=get_user,
        provide_context=True,
    )

    process_user_task = PythonOperator(
        task_id="process_user",
        python_callable=process_user,
        provide_context=True,
    )

    create_table_task = CustomPostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            firstname VARCHAR NOT NULL,
            lastname VARCHAR NOT NULL,
            email VARCHAR PRIMARY KEY
        );
        """,
    )

    insert_data_task = CustomPostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres",
        sql="""
        INSERT INTO users (firstname, lastname, email) VALUES
        {% for row in ti.xcom_pull(task_ids='process_user') %}
        ('{{ row.firstname }}', '{{ row.lastname }}', '{{ row.email }}'){% if not loop.last %}, {% endif %}
        {% endfor %}
        ON CONFLICT (email) DO NOTHING;
        """,
    )

    get_user_task >> process_user_task >> create_table_task >> insert_data_task