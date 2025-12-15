import httpx
from prefect import flow, task
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch


@task(retries=3, retry_delay_seconds=5)
def get_user() -> Dict[str, Any]:
    """Fetch user data from reqres API."""
    url = "https://reqres.in/api/users"
    try:
        response = httpx.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise Exception(f"Failed to fetch user data: {e}")


@task
def process_user(api_response: Dict[str, Any]) -> List[Dict[str, str]]:
    """Process API response and extract user information."""
    users = api_response.get("data", [])
    if not users:
        raise ValueError("No user data found in API response")
    
    processed_users = []
    for user in users:
        processed_users.append({
            "firstname": user.get("first_name", ""),
            "lastname": user.get("last_name", ""),
            "email": user.get("email", "")
        })
    return processed_users


@task
def create_table(conn_string: str):
    """Create users table in PostgreSQL database if it doesn't exist."""
    create_query = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        firstname VARCHAR(100) NOT NULL,
        lastname VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(create_query)
            conn.commit()
    except psycopg2.Error as e:
        raise Exception(f"Failed to create table: {e}")


@task
def insert_data(users: List[Dict[str, str]], conn_string: str):
    """Insert processed user data into PostgreSQL users table."""
    if not users:
        print("No users to insert")
        return
        
    insert_query = """
    INSERT INTO users (firstname, lastname, email)
    VALUES (%(firstname)s, %(lastname)s, %(email)s)
    ON CONFLICT (email) DO NOTHING;
    """
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                execute_batch(cur, insert_query, users)
            conn.commit()
        print(f"Successfully inserted {len(users)} users")
    except psycopg2.Error as e:
        raise Exception(f"Failed to insert data: {e}")


@flow(name="user-data-pipeline")
def user_pipeline(conn_string: str = "postgresql://user:password@localhost:5432/dbname"):
    """
    Daily pipeline to fetch user data from reqres API and store in PostgreSQL.
    
    Args:
        conn_string: PostgreSQL connection string. 
                    Format: postgresql://user:password@host:port/dbname
    """
    # Step 1: Fetch data from API
    api_response = get_user()
    
    # Step 2: Process the data
    processed_users = process_user(api_response)
    
    # Step 3: Create table in database
    create_table(conn_string)
    
    # Step 4: Insert processed data
    insert_data(processed_users, conn_string)


if __name__ == "__main__":
    # Local execution with default connection string
    # For production, use Prefect deployment with schedule:
    # prefect deployment build user_pipeline:user_pipeline --name "daily-user-pipeline" --schedule "0 0 * * *"
    user_pipeline()