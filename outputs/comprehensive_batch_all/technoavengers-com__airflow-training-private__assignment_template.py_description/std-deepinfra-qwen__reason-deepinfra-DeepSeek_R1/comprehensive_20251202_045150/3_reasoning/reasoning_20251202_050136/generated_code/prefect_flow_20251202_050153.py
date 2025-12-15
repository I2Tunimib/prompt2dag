from prefect import flow, task
from typing import Dict, Tuple
import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta

@task
def get_user() -> Dict:
    """Fetch user data from reqres.in API."""
    url = "https://reqres.in/api/users/1"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

@task
def process_user(raw_data: Dict) -> Tuple[str, str, str]:
    """Extract user details from API response."""
    data = raw_data.get("data", {})
    return (
        data.get("first_name", ""),
        data.get("last_name", ""),
        data.get("email", "")
    )

@task
def create_table():
    """Create PostgreSQL users table if not exists."""
    conn = psycopg2.connect(
        host="localhost",
        database="mydb",
        user="user",
        password="password"
    )
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS users "
            "(firstname VARCHAR, lastname VARCHAR, email VARCHAR PRIMARY KEY)"
        )
        conn.commit()
    conn.close()

@task
def insert_data(user_data: Tuple[str, str, str]):
    """Insert processed user data into PostgreSQL."""
    firstname, lastname, email = user_data
    conn = psycopg2.connect(
        host="localhost",
        database="mydb",
        user="user",
        password="password"
    )
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (firstname, lastname, email) "
            "VALUES (%s, %s, %s)",
            (firstname, lastname, email)
        )
        conn.commit()
    conn.close()

@flow
def user_pipeline():
    """Orchestrate user data pipeline with daily schedule."""
    # To schedule daily runs: 
    # prefect deployment build -n user_pipeline_daily --schedule "0 0 * * *"
    api_data = get_user()
    processed_data = process_user(api_data)
    create_table()
    insert_data(processed_data)

if __name__ == "__main__":
    user_pipeline()