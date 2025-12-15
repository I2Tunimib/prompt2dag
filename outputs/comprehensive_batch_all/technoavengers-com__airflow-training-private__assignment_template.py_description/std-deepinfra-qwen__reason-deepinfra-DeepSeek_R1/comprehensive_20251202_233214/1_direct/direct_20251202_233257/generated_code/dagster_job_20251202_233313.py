from dagster import op, job, ResourceDefinition, In, Out, get_dagster_logger
import requests
import psycopg2
from datetime import datetime, timedelta

# Simplified resource stubs
reqres_resource = ResourceDefinition.hardcoded_resource(
    {"base_url": "https://reqres.in/api"}
)

postgres_resource = ResourceDefinition.hardcoded_resource(
    {"dbname": "your_db", "user": "your_user", "password": "your_password", "host": "localhost", "port": "5432"}
)

@op(required_resource_keys={"reqres"}, out=Out(dict))
def get_user(context):
    """Fetch user data from the reqres API."""
    response = requests.get(f"{context.resources.reqres['base_url']}/users/2")
    response.raise_for_status()
    return response.json()["data"]

@op(ins={"user_data": In(dict)}, out=Out(dict))
def process_user(context, user_data):
    """Process the API response to extract user details."""
    processed_data = {
        "firstname": user_data["first_name"],
        "lastname": user_data["last_name"],
        "email": user_data["email"]
    }
    return processed_data

@op(required_resource_keys={"postgres"})
def create_table(context):
    """Create a PostgreSQL users table."""
    conn = psycopg2.connect(**context.resources.postgres)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            firstname TEXT,
            lastname TEXT,
            email TEXT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

@op(ins={"processed_data": In(dict)}, required_resource_keys={"postgres"})
def insert_data(context, processed_data):
    """Insert the processed user data into the PostgreSQL users table."""
    conn = psycopg2.connect(**context.resources.postgres)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (firstname, lastname, email) VALUES (%s, %s, %s)
    """, (processed_data["firstname"], processed_data["lastname"], processed_data["email"]))
    conn.commit()
    cursor.close()
    conn.close()

@job(
    resource_defs={
        "reqres": reqres_resource,
        "postgres": postgres_resource
    },
    description="Pipeline to fetch user data from reqres API, process it, and insert into PostgreSQL."
)
def user_pipeline():
    processed_data = process_user(get_user())
    create_table()
    insert_data(processed_data)

if __name__ == '__main__':
    result = user_pipeline.execute_in_process()