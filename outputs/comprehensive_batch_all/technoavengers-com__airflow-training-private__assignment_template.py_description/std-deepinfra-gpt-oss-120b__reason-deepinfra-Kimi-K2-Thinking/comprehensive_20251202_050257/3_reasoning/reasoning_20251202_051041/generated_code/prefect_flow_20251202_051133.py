import os
from typing import List, Dict, Any
import requests
import psycopg2
from prefect import flow, task
from prefect.blocks.system import Secret


@task
def get_user(api_base_url: str = "https://reqres.in/api") -> Dict[str, Any]:
    """Fetch user data from the reqres API."""
    response = requests.get(f"{api_base_url}/users", timeout=30)
    response.raise_for_status()
    return response.json()


@task
def process_user(api_response: Dict[str, Any]) -> List[Dict[str, str]]:
    """Process API response and extract user information."""
    return [
        {
            "firstname": user.get("first_name", ""),
            "lastname": user.get("last_name", ""),
            "email": user.get("email", "")
        }
        for user in api_response.get("data", [])
    ]


@task
def create_table(connection_string: str):
    """Create PostgreSQL users table if it doesn't exist."""
    conn = psycopg2.connect(connection_string)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    firstname VARCHAR(100),
                    lastname VARCHAR(100),
                    email VARCHAR(255) UNIQUE
                )
            """)
            conn.commit()
    finally:
        conn.close()


@task
def insert_data(users: List[Dict[str, str]], connection_string: str):
    """Insert processed user data into PostgreSQL table."""
    if not users:
        return

    conn = psycopg2.connect(connection_string)
    try:
        with conn.cursor() as cur:
            for user in users:
                cur.execute(
                    """
                    INSERT INTO users (firstname, lastname, email)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    (user["firstname"], user["lastname"], user["email"])
                )
            conn.commit()
    finally:
        conn.close()


@flow
def user_data_pipeline():
    """
    Daily pipeline to fetch user data from reqres API and store in PostgreSQL.
    
    Schedule: Daily at midnight
    Start Date: One day prior to deployment
    """
    try:
        postgres_conn = Secret.load("postgres-connection-string").get()
    except ValueError:
        postgres_conn = os.getenv(
            "POSTGRES_CONNECTION_STRING",
            "postgresql://user:password@localhost:5432/dbname"
        )

    api_response = get_user()
    processed_users = process_user(api_response)
    create_table(postgres_conn)
    insert_data(processed_users, postgres_conn)


if __name__ == "__main__":
    user_data_pipeline()