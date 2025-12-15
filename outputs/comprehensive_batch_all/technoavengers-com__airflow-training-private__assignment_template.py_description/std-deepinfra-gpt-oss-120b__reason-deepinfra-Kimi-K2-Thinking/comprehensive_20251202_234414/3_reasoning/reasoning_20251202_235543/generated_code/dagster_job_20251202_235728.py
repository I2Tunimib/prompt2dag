import requests
import psycopg2
from dagster import op, job, resource, Field
from typing import List, Dict, Any, Generator


@resource(config_schema={
    "host": Field(str, default_value="localhost"),
    "port": Field(int, default_value=5432),
    "database": Field(str, default_value="users_db"),
    "user": Field(str, default_value="postgres"),
    "password": Field(str, default_value="password"),
})
def postgres_resource(context) -> Generator[psycopg2.extensions.connection, None, None]:
    """Provides PostgreSQL connection."""
    conn = psycopg2.connect(
        host=context.resource_config["host"],
        port=context.resource_config["port"],
        database=context.resource_config["database"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
    )
    try:
        yield conn
    finally:
        conn.close()


@op
def get_user() -> Dict[str, Any]:
    """Fetches user data from reqres API."""
    url = "https://reqres.in/api/users"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


@op
def process_user(api_response: Dict[str, Any]) -> List[tuple]:
    """Extracts first name, last name, and email from API response."""
    users = api_response.get("data", [])
    return [
        (user.get("first_name", ""), user.get("last_name", ""), user.get("email", ""))
        for user in users
    ]


@op(required_resource_keys={"postgres"})
def create_table(context) -> str:
    """Creates users table in PostgreSQL."""
    conn = context.resources.postgres
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            email VARCHAR(255) UNIQUE
        );
    """)
    conn.commit()
    cursor.close()
    return "table_created"


@op(required_resource_keys={"postgres"})
def insert_data(context, processed_data: List[tuple], table_status: str) -> None:
    """Inserts processed user data into PostgreSQL table."""
    conn = context.resources.postgres
    cursor = conn.cursor()
    insert_sql = "INSERT INTO users (firstname, lastname, email) VALUES (%s, %s, %s) ON CONFLICT (email) DO NOTHING;"
    for user_data in processed_data:
        cursor.execute(insert_sql, user_data)
    conn.commit()
    cursor.close()


@job(resource_defs={"postgres": postgres_resource})
def user_pipeline():
    """Daily user data pipeline from API to PostgreSQL."""
    user_data = get_user()
    processed_data = process_user(user_data)
    table_status = create_table()
    insert_data(processed_data, table_status)


if __name__ == "__main__":
    result = user_pipeline.execute_in_process()