from datetime import datetime, timedelta
import json
import logging
import requests

import dagster
from dagster import (
    Config,
    ConfigurableResource,
    InitResourceContext,
    In,
    Nothing,
    Out,
    OpExecutionContext,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    ScheduleResult,
    ScheduleStatus,
    job,
    op,
    schedule,
)


class PostgresResource(ConfigurableResource):
    """Simple PostgreSQL resource using psycopg2."""

    db_url: str

    def get_connection(self):
        import psycopg2

        return psycopg2.connect(self.db_url)


@op(out=Out(dict))
def get_user(context: OpExecutionContext) -> dict:
    """Fetch user data from the ReqRes API."""
    url = "https://reqres.in/api/users"
    params = {"per_page": 10}
    context.log.info(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    context.log.debug(f"Received data: {json.dumps(data)[:200]}")
    return data


@op(ins={"api_response": In(dict)}, out=Out(list))
def process_user(context: OpExecutionContext, api_response: dict) -> list:
    """Extract first name, last name, and email from API response."""
    users = api_response.get("data", [])
    processed = [
        {
            "firstname": user.get("first_name", ""),
            "lastname": user.get("last_name", ""),
            "email": user.get("email", ""),
        }
        for user in users
    ]
    context.log.info(f"Processed {len(processed)} users")
    return processed


@op(required_resource_keys={"postgres"}, out=Out(Nothing))
def create_table(context: OpExecutionContext) -> Nothing:
    """Create the users table in PostgreSQL if it does not exist."""
    conn = context.resources.postgres.get_connection()
    try:
        with conn.cursor() as cur:
            create_sql = """
                CREATE TABLE IF NOT EXISTS users (
                    firstname TEXT,
                    lastname TEXT,
                    email TEXT
                );
            """
            context.log.info("Creating users table if not exists")
            cur.execute(create_sql)
            conn.commit()
    finally:
        conn.close()
    return Nothing


@op(
    required_resource_keys={"postgres"},
    ins={"users": In(list)},
    out=Out(Nothing),
)
def insert_data(context: OpExecutionContext, users: list) -> Nothing:
    """Insert processed user data into the PostgreSQL users table."""
    if not users:
        context.log.warning("No user data to insert")
        return Nothing

    conn = context.resources.postgres.get_connection()
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO users (firstname, lastname, email)
                VALUES (%s, %s, %s);
            """
            records = [(u["firstname"], u["lastname"], u["email"]) for u in users]
            context.log.info(f"Inserting {len(records)} records into users table")
            cur.executemany(insert_sql, records)
            conn.commit()
    finally:
        conn.close()
    return Nothing


@job(resource_defs={"postgres": PostgresResource})
def user_pipeline_job():
    """Linear pipeline: fetch → process → create table → insert."""
    raw = get_user()
    processed = process_user(raw)
    create_table()
    insert_data(processed)


@schedule(
    cron_schedule="@daily",
    job=user_pipeline_job,
    execution_timezone="UTC",
    default_status=ScheduleStatus.RUNNING,
)
def daily_user_pipeline_schedule(_context: ScheduleEvaluationContext) -> dict:
    """Daily schedule that runs the user pipeline."""
    return {}


if __name__ == "__main__":
    result = user_pipeline_job.execute_in_process(
        run_config={
            "resources": {
                "postgres": {
                    "config": {
                        "db_url": "postgresql://user:password@localhost:5432/mydb"
                    }
                }
            }
        }
    )
    if result.success:
        logging.info("Pipeline executed successfully")
    else:
        logging.error("Pipeline execution failed")