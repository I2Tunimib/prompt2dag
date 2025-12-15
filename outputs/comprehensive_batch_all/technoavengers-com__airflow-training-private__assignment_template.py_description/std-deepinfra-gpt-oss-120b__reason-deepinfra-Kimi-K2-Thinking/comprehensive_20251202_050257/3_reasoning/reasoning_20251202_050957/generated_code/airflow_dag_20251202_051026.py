from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import requests
import json


class CustomPostgresOperator(PostgresOperator):
    """Extends PostgresOperator to support template fields in parameters."""
    template_fields = ('sql', 'parameters')


def get_user(**context):
    """Fetch user data from reqres API and push to XCom."""
    response = requests.get('https://reqres.in/api/users/1')
    response.raise_for_status()
    context['task_instance'].xcom_push(key='user_data', value=response.text)


def process_user(**context):
    """Process API response and extract user fields."""
    user_data_json = context['task_instance'].xcom_pull(
        task_ids='get_user',
        key='user_data'
    )
    user_data = json.loads(user_data_json)
    user = user_data['data']
    
    processed_user = {
        'firstname': user['first_name'],
        'lastname': user['last_name'],
        'email': user['email']
    }
    
    context['task_instance'].xcom_push(
        key='processed_user',
        value=processed_user
    )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'user_data_pipeline',
    default_args=default_args,
    description='Pipeline to fetch user data from reqres API and store in PostgreSQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['user', 'api', 'postgres'],
) as dag:

    get_user_task = PythonOperator(
        task_id='get_user',
        python_callable=get_user,
    )

    process_user_task = PythonOperator(
        task_id='process_user',
        python_callable=process_user,
    )

    create_table_task = CustomPostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                firstname VARCHAR(255),
                lastname VARCHAR(255),
                email VARCHAR(255) PRIMARY KEY
            );
        """,
    )

    insert_data_task = CustomPostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres',
        sql="""
            INSERT INTO users (firstname, lastname, email)
            VALUES (%(firstname)s, %(lastname)s, %(email)s)
            ON CONFLICT (email) DO NOTHING;
        """,
        parameters={
            'firstname': "{{ task_instance.xcom_pull(task_ids='process_user', key='processed_user')['firstname'] }}",
            'lastname': "{{ task_instance.xcom_pull(task_ids='process_user', key='processed_user')['lastname'] }}",
            'email': "{{ task_instance.xcom_pull(task_ids='process_user', key='processed_user')['email'] }}",
        },
    )

    get_user_task >> process_user_task >> create_table_task >> insert_data_task