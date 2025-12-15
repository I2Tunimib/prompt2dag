import os
import requests
import psycopg2
from prefect import flow, task
from typing import Dict, Any

# Configuration via environment variables with safe defaults
DEFAULT_POSTGRES_CONN = os.getenv(
    "POSTGRES_CONNECTION_STRING",
    "postgresql://user:password@localhost:5432/prefect_db"
)
REQRES_API_URL = os.getenv("REQRES_API_URL", "https://reqres.in/api/users/1")


@task(retries=2, retry_delay_seconds=30)
def get_user_task(api_url: str = REQRES_API_URL) -> Dict[str, Any]:
    """Fetches user data from the reqres API endpoint."""
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise Exception(f"Failed to fetch user data: {e}")


@task
def process_user_task(user_data: Dict[str, Any]) -> Dict[str, str]:
    """Processes the API response and extracts user information."""
    try:
        user = user_data.get('data', {})
        return {
            'firstname': user.get('first_name', ''),
            'lastname': user.get('last_name', ''),
            'email': user.get('email', '')
        }
    except Exception as e:
        raise Exception(f"Failed to process user data: {e}")


@task
def create_table_task(connection_string: str) -> None:
    """Creates the users table in PostgreSQL if it doesn't exist."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            firstname VARCHAR(100) NOT NULL,
            lastname VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
    except psycopg2.Error as e:
        raise Exception(f"Failed to create table: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@task
def insert_data_task(processed_data: Dict[str, str], connection_string: str) -> str:
    """Inserts processed user data into the PostgreSQL users table."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        
        insert_sql = """
        INSERT INTO users (firstname, lastname, email)
        VALUES (%s, %s, %s)
        ON CONFLICT (email) DO NOTHING;
        """
        
        cursor.execute(
            insert_sql,
            (
                processed_data['firstname'],
                processed_data['lastname'],
                processed_data['email']
            )
        )
        
        conn.commit()
        return f"Inserted user: {processed_data['email']}"
    except psycopg2.Error as e:
        raise Exception(f"Failed to insert data: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@flow
def user_data_pipeline(
    connection_string: str = DEFAULT_POSTGRES_CONN,
    api_url: str = REQRES_API_URL
) -> None:
    """
    Daily pipeline to fetch user data from reqres API and store in PostgreSQL.
    
    Schedule: Daily (configure via deployment)
    Start Date: One day prior to deployment (configure via deployment)
    """
    # Sequential execution: get → process → create → insert
    user_data = get_user_task(api_url)
    processed_data = process_user_task(user_data)
    create_table_task(connection_string)
    insert_data_task(processed_data, connection_string)


if __name__ == '__main__':
    # Local execution for testing
    # For production deployment with daily schedule:
    # prefect deployment build user_data_pipeline:user_data_pipeline \
    #   --name "daily-user-pipeline" \
    #   --cron "0 0 * * *" \
    #   --param connection_string="your-connection-string"
    user_data_pipeline()