from typing import Any, Dict, List

import psycopg2
import requests
from dagster import (
    GraphDefinition,
    ScheduleDefinition,
    JobDefinition,
    OpExecutionContext,
    GraphOut,
    op,
    graph,
    resource,
    job,
)


@resource
def http_resource(_init_context) -> requests.Session:
    """Simple HTTP resource using requests.Session."""
    return requests.Session()


@resource
def postgres_resource(init_context) -> psycopg2.extensions.connection:
    """PostgreSQL connection resource.

    Expected config:
        {
            "dbname": str,
            "user": str,
            "password": str,
            "host": str,
            "port": int (optional, defaults to 5432)
        }
    """
    cfg = init_context.resource_config
    conn = psycopg2.connect(
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=cfg.get("port", 5432),
    )
    return conn


@op(required_resource_keys={"http"})
def get_user(context: OpExecutionContext) -> Dict[str, Any]:
    """Fetch user data from the ReqRes API."""
    url = "https://reqres.in/api/users"
    response = context.resources.http.get(url)
    response.raise_for_status()
    data = response.json()
    context.log.info(f"Fetched data: {data}")
    return data


@op
def process_user(context: OpExecutionContext, api_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract first name, last name, and email from API response."""
    users = api_data.get("data", [])
    processed = [
        {
            "first_name": user["first_name"],
            "last_name": user["last_name"],
            "email": user["email"],
        }
        for user in users
    ]
    context.log.info(f"Processed {len(processed)} users")
    return processed


@op(required_resource_keys={"postgres"})
def create_table(context: OpExecutionContext) -> None:
    """Create the users table in PostgreSQL if it does not exist."""
    conn = context.resources.postgres
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255)
            );
            """
        )
        conn.commit()
    context.log.info("Ensured users table exists")


@op(required_resource_keys={"postgres"})
def insert_data(context: OpExecutionContext, users: List[Dict[str, str]]) -> None:
    """Insert processed user data into the PostgreSQL users table."""
    conn = context.resources.postgres
    with conn.cursor() as cur:
        for user in users:
            cur.execute(
                """
                INSERT INTO users (first_name, last_name, email)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (user["first_name"], user["last_name"], user["email"]),
            )
        conn.commit()
    context.log.info(f"Inserted {len(users)} users into the database")


@graph
def user_pipeline_graph() -> GraphOut:
    """Define the linear pipeline: get → process → create table → insert."""
    raw = get_user()
    processed = process_user(raw)
    create = create_table()
    insert = insert_data(processed)

    # Enforce execution order
    create.after(processed)
    insert.after(create)

    return insert


user_job: JobDefinition = user_pipeline_graph.to_job(
    resource_defs={"http": http_resource, "postgres": postgres_resource}
)


daily_user_job_schedule = ScheduleDefinition(
    job=user_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
    name="daily_user_job_schedule",
)


if __name__ == "__main__":
    result = user_job.execute_in_process(
        run_config={
            "resources": {
                "http": {"config": {}},
                "postgres": {
                    "config": {
                        "dbname": "mydb",
                        "user": "myuser",
                        "password": "mypassword",
                        "host": "localhost",
                        "port": 5432,
                    }
                },
            }
        }
    )
    assert result.success, "Pipeline execution failed"