from dagster import op, job, get_dagster_logger, In, Out, Nothing
import requests
import psycopg2
from datetime import datetime, timedelta

# Resources
class ReqresResource:
    def __init__(self, base_url):
        self.base_url = base_url

class PostgresResource:
    def __init__(self, conn_info):
        self.conn_info = conn_info

    def get_connection(self):
        return psycopg2.connect(**self.conn_info)

# Ops
@op(required_resource_keys={"reqres"})
def get_user(context) -> dict:
    """Fetch user data from the reqres API."""
    response = requests.get(f"{context.resources.reqres.base_url}/api/users/2")
    response.raise_for_status()
    return response.json()

@op(ins={"user_data": In(dict)}, out=Out(dict))
def process_user(context, user_data) -> dict:
    """Process the API response by extracting relevant fields."""
    processed_data = {
        "firstname": user_data["data"]["first_name"],
        "lastname": user_data["data"]["last_name"],
        "email": user_data["data"]["email"]
    }
    return processed_data

@op(required_resource_keys={"postgres"})
def create_table(context) -> Nothing:
    """Create a PostgreSQL users table."""
    conn = context.resources.postgres.get_connection()
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
def insert_data(context, processed_data) -> Nothing:
    """Insert the processed user data into the PostgreSQL users table."""
    conn = context.resources.postgres.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (firstname, lastname, email) VALUES (%s, %s, %s)
    """, (processed_data["firstname"], processed_data["lastname"], processed_data["email"]))
    conn.commit()
    cursor.close()
    conn.close()

# Job
@job(
    resource_defs={
        "reqres": ReqresResource(base_url="https://reqres.in"),
        "postgres": PostgresResource(conn_info={
            "dbname": "your_db",
            "user": "your_user",
            "password": "your_password",
            "host": "your_host",
            "port": "your_port"
        })
    }
)
def user_pipeline():
    """A pipeline to fetch, process, and store user data."""
    processed_data = process_user(get_user())
    insert_data(create_table(), processed_data)

# Launch pattern
if __name__ == '__main__':
    result = user_pipeline.execute_in_process()