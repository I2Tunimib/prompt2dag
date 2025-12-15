from __future__ import annotations

import json
from typing import Any, List, Mapping

import requests
import psycopg2
from dagster import (
    DefaultScheduleStatus,
    OpExecutionContext,
    ScheduleDefinition,
    GraphDefinition,
    JobDefinition,
    ResourceDefinition,
    op,
    graph,
    job,
)


def http_client_resource(_init_context) -> requests.Session:
    """Simple HTTP client resource using ``requests``."""
    session = requests.Session()
    return session


class PostgresResource:
    """Minimal PostgreSQL resource wrapper."""

    def __init__(self, conn_str: str):
        self.conn = psycopg2.connect(conn_str)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def run_sql(self, sql: str, params: Any = None) -> None:
        """Execute a SQL statement."""
        self.cur.execute(sql, params)

    def close(self) -> None:
        self.cur.close()
        self.conn.close()


def postgres_resource(init_context) -> PostgresResource:
    """PostgreSQL resource using a connection string from config."""
    conn_str = init_context.resource_config.get(
        "conn_str", "postgresql://user:password@localhost:5432/mydb"
    )
    return PostgresResource(conn_str)


@op(required_resource_keys={"http_client"})
def get_user(context: OpExecutionContext) -> Mapping[str, Any]:
    """Fetch user data from the ReqRes API."""
    url = "https://reqres.in/api/users"
    response = context.resources.http_client.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


@op
def process_user(context: OpExecutionContext, api_response: Mapping[str, Any]) -> List[Mapping[str, str]]:
    """Extract first name, last name, and email from the API response."""
    users = api_response.get("data", [])
    processed = [
        {
            "firstname": user.get("first_name", ""),
            "lastname": user.get("last_name", ""),
            "email": user.get("email", ""),
        }
        for user in users
    ]
    context.log.info(f"Processed {len(processed)} users.")
    return processed


@op(required_resource_keys={"postgres"})
def create_table(context: OpExecutionContext) -> None:
    """Create the ``users`` table if it does not already exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS users (
        firstname TEXT NOT NULL,
        lastname TEXT NOT NULL,
        email TEXT PRIMARY KEY
    );
    """
    context.resources.postgres.run_sql(create_sql)
    context.log.info("Table ``users`` ensured.")


@op(required_resource_keys={"postgres"})
def insert_data(context: OpExecutionContext, users: List[Mapping[str, str]]) -> None:
    """Insert processed user records into the ``users`` table."""
    insert_sql = """
    INSERT INTO users (firstname, lastname, email)
    VALUES (%s, %s, %s)
    ON CONFLICT (email) DO UPDATE
    SET firstname = EXCLUDED.firstname,
        lastname = EXCLUDED.lastname;
    """
    for user in users:
        params = (user["firstname"], user["lastname"], user["email"])
        context.resources.postgres.run_sql(insert_sql, params)
    context.log.info(f"Inserted/updated {len(users)} user records.")


@graph
def user_pipeline_graph():
    """Linear pipeline: fetch → process → create table → insert."""
    raw = get_user()
    processed = process_user(raw)
    create_table()
    insert_data(processed)


user_job: JobDefinition = user_pipeline_graph.to_job(
    name="user_pipeline_job",
    resource_defs={
        "http_client": ResourceDefinition.resource(http_client_resource),
        "postgres": ResourceDefinition.resource(
            postgres_resource,
            config_schema={"conn_str": str},
        ),
    },
)


daily_user_schedule = ScheduleDefinition(
    job=user_job,
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily execution of the user pipeline.",
)


if __name__ == "__main__":
    result = user_job.execute_in_process()
    print(f"Pipeline succeeded: {result.success}")