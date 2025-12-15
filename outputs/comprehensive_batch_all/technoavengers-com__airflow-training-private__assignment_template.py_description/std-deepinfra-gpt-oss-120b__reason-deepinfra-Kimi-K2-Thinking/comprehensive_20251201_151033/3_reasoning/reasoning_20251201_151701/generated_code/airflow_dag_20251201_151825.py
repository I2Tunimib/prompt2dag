"""
Daily pipeline to fetch user data from reqres API and store in PostgreSQL.
Linear execution: get_user → process_user → create_table → insert_data
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import requests


class CustomPostgresOperator(PostgresOperator):
    """Extended PostgresOperator with template field support for parameters."""
    template_fields = ('sql', 'parameters')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='user_data_pipeline',
    default_args=default_args,
    description='Fetch user data from reqres API and store in PostgreSQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['user', 'api', 'postgres'],
) as dag:

    def get_user(**context):
        """Fetch user data from reqres API"""
        response = requests.get('https://reqres.in/api/users/1')
        response.raise_for_status()
        user_data = response.json()
        context['task_instance'].xcom_push(key='user_data', value=user_data)

    def process_user(**context):
        """Process user data and extract firstname, lastname, email"""
        user_data = context['task_instance'].xcom_pull(
            task_ids='get_user',
            key='user_data'
        )
        
        if not user_data or 'data' not in user_data:
            raise ValueError("No user data found in XCom")
        
        user = user_data['data']
        processed_data = {
            'firstname': user.get('first_name'),
            'lastname': user.get('last_name'),
            'email': user.get('email')
        }
        
        context['task_instance'].xcom_push(
            key='processed_user',
            value=processed_data
        )

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
                firstname VARCHAR(100),
                lastname VARCHAR(100),
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
        parameters="{{ ti.xcom_pull(task_ids='process_user', key='processed_user') }}",
    )

    get_user_task >> process_user_task >> create_table_task >> insert_data_task