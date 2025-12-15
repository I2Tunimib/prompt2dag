from datetime import datetime, timedelta
from prefect import flow, task
import requests
import psycopg2
from psycopg2.extras import execute_values

@task
def get_user():
    """Fetch user data from the reqres API."""
    response = requests.get("https://reqres.in/api/users")
    response.raise_for_status()
    return response.json()

@task
def process_user(user_data):
    """Process the API response by parsing JSON data and extracting required fields."""
    users = user_data.get("data", [])
    processed_users = [
        (user["first_name"], user["last_name"], user["email"]) for user in users
    ]
    return processed_users

@task
def create_table():
    """Create a PostgreSQL users table."""
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            firstname TEXT,
            lastname TEXT,
            email TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

@task
def insert_data(processed_users):
    """Insert the processed user data into the PostgreSQL users table."""
    conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpassword host=yourhost")
    cur = conn.cursor()
    execute_values(cur, "INSERT INTO users (firstname, lastname, email) VALUES %s", processed_users)
    conn.commit()
    cur.close()
    conn.close()

@flow(name="User Data Pipeline")
def user_data_pipeline():
    """Orchestrates the user data pipeline."""
    user_data = get_user()
    processed_users = process_user(user_data)
    create_table()
    insert_data(processed_users)

if __name__ == '__main__':
    user_data_pipeline()

# Deployment/schedule configuration (optional)
# Schedule: Daily execution (@daily)
# Start Date: One day prior to current date (days_ago(1))