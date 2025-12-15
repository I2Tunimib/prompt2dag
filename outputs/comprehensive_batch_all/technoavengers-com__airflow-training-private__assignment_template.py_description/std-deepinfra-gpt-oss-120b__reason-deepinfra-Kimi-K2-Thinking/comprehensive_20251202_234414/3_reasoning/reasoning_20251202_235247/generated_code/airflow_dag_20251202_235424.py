from datetime import timedelta
import json
from typing import Dict, List, Any

from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


class CustomPostgresOperator(PostgresOperator):
    """Extended PostgresOperator with template field support for parameters."""
    
    template_fields = ('sql', 'parameters')


def get_user(**kwargs) -> str:
    """Fetch user data from reqres API and push to XCom."""
    hook = HttpHook(method='GET', http_conn_id='reqres')
    response = hook.run(endpoint='api/users?page=1')
    response.raise_for_status()
    
    kwargs['ti'].xcom_push(key='user_data_raw', value=response.text)
    return response.text


def process_user(**kwargs) -> List[Dict[str, str]]:
    """Process raw API response and extract user fields."""
    raw_data = kwargs['ti'].xcom_pull(
        task_ids='get_user',
        key='user_data_raw'
    )
    
    if not raw_data:
        raise ValueError("No data received from get_user task")
    
    data = json.loads(raw_data)
    users = data.get('data', [])
    
    processed_users = []
    for user in users:
        processed_users.append({
            'firstname': user.get('first_name', ''),
            'lastname': user.get('last_name', ''),
            'email': user.get('email', '')
        })
    
    kwargs['ti'].xcom_push(key='processed_users', value=processed_users)
    return processed_users


def insert_data(**kwargs):
    """Insert processed user data into PostgreSQL table."""
    processed_users = kwargs['ti'].xcom_pull(
        task_ids='process_user',
        key='processed_users'
    )
    
    if not processed_users:
        raise ValueError("No processed data received from process_user task")
    
    hook = PostgresHook(postgres_conn_id='postgres')
    
    for user in processed_users:
        sql = """
        INSERT INTO users (firstname, lastname, email)
        VALUES (%(firstname)s, %(lastname)s, %(email)s)
        ON CONFLICT (email) DO NOTHING;
        """
        hook.run(sql, parameters=user)


default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='user_data_pipeline',
    default_args=default_args,
    description='Pipeline to fetch user data from reqres API and store in PostgreSQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['users', 'api', 'postgres'],
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
            id SERIAL PRIMARY KEY,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            email VARCHAR(255) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )
    
    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
    )
    
    get_user_task >> process_user_task >> create_table_task >> insert_data_task