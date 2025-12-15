"""Airflow DAG to fetch user data from an API, process it, and store it in PostgreSQL."""

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def process_user(**context):
    """Parse API JSON response and extract user fields."""
    response = context["ti"].xcom_pull(task_ids="get_user")
    if not response:
        raise ValueError("No response received from get_user task.")
    # SimpleHttpOperator returns the response text; parse JSON.
    import json

    data = json.loads(response)
    users = data.get("data", [])
    processed = [
        {
            "first_name": user.get("first_name"),
            "last_name": user.get("last_name"),
            "email": user.get("email"),
        }
        for user in users
    ]
    # Push processed list to XCom
    return processed


def insert_data(**context):
    """Insert processed user data into PostgreSQL."""
    processed = context["ti"].xcom_pull(task_ids="process_user")
    if not processed:
        raise ValueError("No processed data found in XCom.")
    hook = PostgresHook(postgres_conn_id="postgres")
    insert_sql = """
        INSERT INTO users (firstname, lastname, email)
        VALUES (%s, %s, %s)
        ON CONFLICT (email) DO NOTHING;
    """
    rows = [
        (item["first_name"], item["last_name"], item["email"])
        for item in processed
        if item["email"]
    ]
    if rows:
        hook.run(insert_sql, parameters=rows, autocommit=True)


class CustomPostgresOperator(PostgresOperator):
    """Custom PostgresOperator with explicit template fields."""

    template_fields = ("sql",)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_data_pipeline",
    default_args=default_args,
    description="Fetch, process, and store user data daily.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
) as dag:
    get_user = SimpleHttpOperator(
        task_id="get_user",
        http_conn_id="reqres",
        endpoint="/api/users?page=1",
        method="GET",
        log_response=True,
        response_filter=lambda response: response.text,
        extra_options={"timeout": 30},
    )

    process_user_task = PythonOperator(
        task_id="process_user",
        python_callable=process_user,
        provide_context=True,
    )

    create_table = CustomPostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                firstname VARCHAR(50),
                lastname VARCHAR(50),
                email VARCHAR(100) UNIQUE
            );
        """,
    )

    insert_data_task = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
        provide_context=True,
    )

    # Define linear dependencies
    get_user >> process_user_task >> create_table >> insert_data_task