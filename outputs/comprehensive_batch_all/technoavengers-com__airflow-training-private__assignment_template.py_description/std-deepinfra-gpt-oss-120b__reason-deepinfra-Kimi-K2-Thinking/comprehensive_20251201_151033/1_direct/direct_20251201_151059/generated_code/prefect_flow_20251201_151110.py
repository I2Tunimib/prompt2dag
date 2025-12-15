from __future__ import annotations

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
    processed: List[Dict] = []
    for user in users:
        processed.append(
            {
                "firstname": user.get("first_name"),
                "lastname": user.get("last_name"),
                "email": user.get("email"),
            }
        )
    return processed


def _get_pg_connection() -> psycopg2.extensions.connection:
    """Create a PostgreSQL connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


@task
def create_table() -> None:
    """Create the users table in PostgreSQL if it does not already exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255)
    );
    """
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
    finally:
        conn.close()


@task
def insert_data(users: List[Dict]) -> None:
    """Insert processed user data into the PostgreSQL users table."""
    insert_sql = """
    INSERT INTO users (firstname, lastname, email)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            for user in users:
                cur.execute(
                    insert_sql,
                    (user["firstname"], user["lastname"], user["email"]),
                )
        conn.commit()
    finally:
        conn.close()


@flow
def user_pipeline() -> None:
    """
    Orchestrate fetching, processing, and storing user data.

    Schedule: Daily execution (e.g., via Prefect deployment with a cron schedule).
    """
    api_response = get_user()
    processed_users = process_user(api_response)
    create_table()
    insert_data(processed_users)


if __name__ == "__main__":
    user_pipeline()