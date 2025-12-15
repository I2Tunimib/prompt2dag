from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import XCom
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_user_data(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='get_user')
    user_data = json.loads(response)
    processed_data = {
        'firstname': user_data['data']['first_name'],
        'lastname': user_data['data']['last_name'],
        'email': user_data['data']['email']
    }
    ti.xcom_push(key='processed_user_data', value=processed_data)

def insert_data_into_postgres(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(task_ids='process_user', key='processed_user_data')
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    query = """
    INSERT INTO users (firstname, lastname, email) VALUES (%s, %s, %s);
    """
    postgres_hook.run(query, parameters=(processed_data['firstname'], processed_data['lastname'], processed_data['email']))

with DAG(
    'user_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch, process, and store user data',
    schedule_interval='@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
) as dag:

    get_user = SimpleHttpOperator(
        task_id='get_user',
        http_conn_id='reqres',
        endpoint='api/users/2',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=process_user_data,
        provide_context=True
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255)
        );
        """
    )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_into_postgres,
        provide_context=True
    )

    get_user >> process_user >> create_table >> insert_data