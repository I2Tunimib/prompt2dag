import os
from typing import List, Dict

import psycopg2
import requests
from prefect import flow, task


@task
def get_user() -> Dict:
    """Fetch user data from the ReqRes API."""
    url = "https://reqres.in/api/users"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@task
def process_user(api_response: Dict) -> List[Dict]:
    """Extract first name, last name, and email from the API response."""
    users = api_response.get("data", [])
    processed = [
        {
            "firstname": user.get("first_name", ""),
            "lastname": user.get("last_name", ""),
            "email": user.get("email", ""),
        }
        for user in users
    ]
    return processed


def _get_db_connection():
    """Create a PostgreSQL connection using environment variables."""
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )
    return conn


@task
def create_table():
    """Create the users table if it does not already exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS users (
        firstname TEXT NOT NULL,
        lastname TEXT NOT NULL,
        email TEXT PRIMARY KEY
    );
    """
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
    finally:
        conn.close()


@task
def insert_data(processed_users: List[Dict]):
    """Insert processed user data into the PostgreSQL users table."""
    if not processed_users:
        return

    insert_sql = """
    INSERT INTO users (firstname, lastname, email)
    VALUES (%s, %s, %s)
    ON CONFLICT (email) DO NOTHING;
    """
    values = [
        (user["firstname"], user["lastname"], user["email"])
        for user in processed_users
    ]

    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, values)
        conn.commit()
    finally:
        conn.close()


@flow
def user_pipeline():
    """Orchestrate the user data pipeline."""
    api_response = get_user()
    processed = process_user(api_response)
    create_table()
    insert_data(processed)


if __name__ == "__main__":
    # Deployment schedule (example):
    # - Cron: @daily
    # - Start date: days_ago(1)
    user_pipeline()