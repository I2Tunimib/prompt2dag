from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


class CustomPostgresOperator(PostgresOperator):
    """
    Simple subclass of PostgresOperator that explicitly defines
    ``sql`` as a templated field.
    """

    template_fields = ("sql",)


def fetch_user(**kwargs):
    """
    Retrieve user data from the ReqRes API using an HttpHook.
    The JSON response is pushed to XCom.
    """
    http = HttpHook(http_conn_id="reqres", method="GET")
    # The endpoint returns a list of users; adjust as needed.
    response = http.run(endpoint="/api/users/2")
    response.raise_for_status()
    return response.json()


def process_user(**kwargs):
    """
    Extract first name, last name, and email from the API response.
    The extracted data is returned for downstream XCom usage.
    """
    ti = kwargs["ti"]
    api_response = ti.xcom_pull(task_ids="get_user")
    # Expected structure: {"data": {"first_name": "...", "last_name": "...", "email": "..."}}
    user_data = api_response.get("data", {})
    processed = {
        "firstname": user_data.get("first_name", ""),
        "lastname": user_data.get("last_name", ""),
        "email": user_data.get("email", ""),
    }
    return processed


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
) as dag:
    get_user = PythonOperator(
        task_id="get_user",
        python_callable=fetch_user,
    )

    process_user = PythonOperator(
        task_id="process_user",
        python_callable=process_user,
    )

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        firstname VARCHAR(50),
        lastname VARCHAR(50),
        email VARCHAR(100) PRIMARY KEY
    );
    """

    create_table = CustomPostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql=create_table_sql,
    )

    insert_sql = """
    INSERT INTO users (firstname, lastname, email)
    VALUES (
        '{{ ti.xcom_pull(task_ids="process_user")["firstname"] }}',
        '{{ ti.xcom_pull(task_ids="process_user")["lastname"] }}',
        '{{ ti.xcom_pull(task_ids="process_user")["email"] }}'
    )
    ON CONFLICT (email) DO NOTHING;
    """

    insert_data = CustomPostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres",
        sql=insert_sql,
    )

    # Define linear dependencies
    get_user >> process_user >> create_table >> insert_data