from dagster import op, job, resource, Field
import requests
import psycopg2
from typing import List, Dict, Any

# Resources
@resource(config_schema={
    "base_url": Field(str, default_value="https://reqres.in/api", 
                     description="Reqres API base URL")
})
def reqres_api_resource(init_context):
    """Resource for reqres API connection."""
    return init_context.resource_config


@resource(config_schema={
    "host": Field(str, default_value="localhost", description="PostgreSQL host"),
    "port": Field(int, default_value=5432, description="PostgreSQL port"),
    "dbname": Field(str, default_value="users_db", description="Database name"),
    "user": Field(str, default_value="postgres", description="Database user"),
    "password": Field(str, default_value="password", description="Database password")
})
def postgres_resource(init_context):
    """Resource for PostgreSQL database connection."""
    return init_context.resource_config


# Ops
@op
def get_user(context) -> Dict[str, Any]:
    """
    Executes an HTTP GET request to the reqres API endpoint 
    to retrieve user data in JSON format.
    """
    api_config = context.resources.reqres_api_resource
    url = f"{api_config['base_url']}/users"
    
    context.log.info(f"Fetching user data from {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    user_count = len(data.get("data", []))
    context.log.info(f"Successfully fetched {user_count} users")
    
    return data


@op
def process_user(context, api_response: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Processes the API response by parsing JSON data and extracting 
    first name, last name, and email fields.
    """
    context.log.info("Processing user data")
    
    users = api_response.get("data", [])
    processed_users = []
    
    for user in users:
        processed_users.append({
            "firstname": user.get("first_name", ""),
            "lastname": user.get("last_name", ""),
            "email": user.get("email", "")
        })
    
    context.log.info(f"Processed {len(processed_users)} users")
    return processed_users


@op
def create_table(context, _processed_data: List[Dict[str, str]]) -> None:
    """
    Creates a PostgreSQL users table with columns for firstname, lastname, and email.
    
    Note: _processed_data is only used for dependency ordering to maintain 
    the linear execution flow. The actual data is not used in this step.
    """
    context.log.info("Creating PostgreSQL users table")
    
    pg_config = context.resources.postgres_resource
    
    try:
        conn = psycopg2.connect(**pg_config, connect_timeout=10)
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            email VARCHAR(255) UNIQUE
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        context.log.info("Users table created successfully")
        
    except Exception as e:
        context.log.error(f"Error creating table: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


@op
def insert_data(context, processed_data: List[Dict[str, str]], 
                _table_created: None) -> None:
    """
    Inserts the processed user data into the PostgreSQL users table.
    
    Note: _table_created is only used for dependency ordering to ensure 
    this step runs after table creation.
    """
    context.log.info(f"Inserting {len(processed_data)} users into database")
    
    if not processed_data:
        context.log.info("No data to insert")
        return
    
    pg_config = context.resources.postgres_resource
    
    try:
        conn = psycopg2.connect(**pg_config, connect_timeout=10)
        cursor = conn.cursor()
        
        insert_sql = """
        INSERT INTO users (firstname, lastname, email)
        VALUES (%(firstname)s, %(lastname)s, %(email)s)
        ON CONFLICT (email) DO NOTHING;
        """
        
        cursor.executemany(insert_sql, processed_data)
        conn.commit()
        
        context.log.info(f"Successfully inserted {cursor.rowcount} users")
        
    except Exception as e:
        context.log.error(f"Error inserting data: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


# Job
@job(
    resource_defs={
        "reqres_api_resource": reqres_api_resource,
        "postgres_resource": postgres_resource
    },
    config={
        "resources": {
            "reqres_api_resource": {
                "config": {
                    "base_url": "https://reqres