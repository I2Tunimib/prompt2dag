import os
from typing import List, Tuple

import psycopg2
import requests
from prefect import flow, task


@task
def get_user() -> dict:
    """Fetch user data from the ReqRes API."""
    url = "https://reqres.in/api/users"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@task
def process_user(api_response: dict) -> List[Tuple[str, str, str]]:
    """Extract first name, last name, and email from the API response."""
    users = api_response.get("data", [])
    processed = [
        (
            user.get("first_name", ""),
            user.get("last_name", ""),
            user.get("email", ""),
        )
        for user in users
    ]
    return processed


@task
def create_table() -> None:
    """Create the users table in PostgreSQL if it does not exist."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    firstname VARCHAR(255),
                    lastname VARCHAR(255),
                    email VARCHAR(255)
                );
                """
            )
    conn.close()


@task
def insert_data(user_data: List[Tuple[str, str, str]]) -> None:
    """Insert processed user data into the PostgreSQL users table."""
    if not user_data:
        return

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    with conn:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO users (firstname, lastname, email)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
            """
            cur.executemany(insert_query, user_data)
    conn.close()


@flow
def user_pipeline() -> None:
    """Orchestrate the user data pipeline.

    Schedule: daily execution (configured in Prefect deployment)
    """
    api_response = get_user()
    processed = process_user(api_response)
    create_table()
    insert_data(processed)


if __name__ == "__main__":
    # Local execution entry point
    user_pipeline()