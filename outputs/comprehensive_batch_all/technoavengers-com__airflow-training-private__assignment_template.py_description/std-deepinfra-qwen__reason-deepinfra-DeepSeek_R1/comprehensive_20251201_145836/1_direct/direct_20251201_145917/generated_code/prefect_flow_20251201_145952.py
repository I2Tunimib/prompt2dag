from prefect import flow, task
import requests
from datetime import datetime, timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

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
        {"firstname": user["first_name"], "lastname": user["last_name"], "email": user["email"]}
        for user in users
    ]
    return processed_users

@task
def create_table():
    """Create a PostgreSQL users table with columns for firstname, lastname, and email."""
    query = """
    CREATE TABLE IF NOT EXISTS users (
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255)
    );
    """
    with SqlAlchemyConnector.load("postgres-connector") as database:
        database.execute(query)

@task
def insert_data(processed_users):
    """Insert the processed user data into the PostgreSQL users table."""
    query = """
    INSERT INTO users (firstname, lastname, email) VALUES (:firstname, :lastname, :email);
    """
    with SqlAlchemyConnector.load("postgres-connector") as database:
        for user in processed_users:
            database.execute(query, **user)

@flow(name="User Data Pipeline")
def user_data_pipeline():
    """Orchestrates the user data pipeline from API fetch to database insertion."""
    user_data = get_user()
    processed_users = process_user(user_data)
    create_table()
    insert_data(processed_users)

if __name__ == "__main__":
    user_data_pipeline()

# Deployment/Schedule Configuration (optional)
# Schedule: Daily execution (@daily)
# Start Date: One day prior to current date (days_ago(1))