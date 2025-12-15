import json
from typing import List, Dict, Any

import requests
import psycopg2
from dagster import op, job, Config, OpExecutionContext


# Minimal config for database connection
class PostgresConfig(Config):
    host: str = "localhost"
    port: int = 5432
    database: str = "mydb"
    user: str = "myuser"
    password: str = "mypassword"


@op
def get_user(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Executes an HTTP GET request to the reqres API endpoint
    to retrieve user data in JSON format.
    """
    url = "https://reqres.in/api/users"
    context.log.info(f"Fetching user data from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        context.log.info(f"Successfully fetched {len(data.get('data', []))} users")
        return data
    except requests.RequestException as e:
        context.log.error(f"Failed to fetch user data: {e}")
        raise


@op
def process_user(context: OpExecutionContext, api_response: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Processes the API response by parsing JSON data and extracting
    first name, last name, and email fields.
    """
    context.log.info("Processing user data from API response")
    
    users = api_response.get("data", [])
    processed_users = []
    
    for user in users:
        first_name = user.get("first_name", "")
        last_name = user.get("last_name", "")
        email = user.get("email", "")
        
        processed_users.append({
            "firstname": first_name,
            "lastname": last_name,
            "email": email
        })
    
    context.log.info(f"Processed {len(processed_users)} users")
    return processed_users


@op
def create_table(context: OpExecutionContext, config: PostgresConfig):
    """
    Creates a PostgreSQL users table with columns for firstname, lastname, and email.
    """
    context.log.info("Creating PostgreSQL users table")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        firstname VARCHAR(100),
        lastname VARCHAR(100),
        email VARCHAR(255) UNIQUE
    );
    """
    
    try:
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()
        context.log.info("Users table created successfully")
    except psycopg2.Error as e:
        context.log.error(f"Failed to create table: {e}")
        raise


@op
def insert_data(
    context: OpExecutionContext, 
    processed_users: List[Dict[str, str]], 
    config: PostgresConfig
):
    """
    Inserts the processed user data into the PostgreSQL users table.
    """
    context.log.info(f"Inserting {len(processed_users)} users into database")
    
    insert_sql = """
    INSERT INTO users (firstname, lastname, email)
    VALUES (%s, %s, %s)
    ON CONFLICT (email) DO NOTHING;
    """
    
    try:
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        cursor = conn.cursor()
        
        for user in processed_users:
            cursor.execute(insert_sql, (user["firstname"], user["lastname"], user["email"]))
        
        conn.commit()
        inserted_count = cursor.rowcount
        cursor.close()
        conn.close()
        
        context.log.info(f"Successfully inserted {inserted_count} new users")
    except psycopg2.Error as e:
        context.log.error(f"Failed to insert data: {e}")
        raise


@job
def user_pipeline():
    """
    Linear pipeline that fetches user data from reqres API,
    processes it, creates a PostgreSQL table, and inserts the data.
    """
    # Linear execution: get_user → process_user → create_table → insert_data
    api_data = get_user()
    processed_users = process_user(api_data)
    create_table()
    insert_data(processed_users)


# Schedule definition (commented out as it requires additional setup)
# from dagster import ScheduleDefinition, Definitions
# 
# user_pipeline_schedule = ScheduleDefinition(
#     job=user_pipeline,
#     cron_schedule="0 0 * * *",  # Daily at midnight
# )
# 
# defs = Definitions(
#     jobs=[user_pipeline],
#     schedules=[user_pipeline_schedule],
# )


if __name__ == "__main__":
    # Example execution with default config
    # In production, use Dagster's config system to provide credentials
    result = user_pipeline.execute_in_process(
        run_config={
            "ops": {
                "create_table": {
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "mydb",
                        "user": "myuser",
                        "password": "mypassword"
                    }
                },
                "insert_data": {
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "mydb",
                        "user": "myuser",
                        "password": "mypassword"
                    }
                }
            }
        }
    )