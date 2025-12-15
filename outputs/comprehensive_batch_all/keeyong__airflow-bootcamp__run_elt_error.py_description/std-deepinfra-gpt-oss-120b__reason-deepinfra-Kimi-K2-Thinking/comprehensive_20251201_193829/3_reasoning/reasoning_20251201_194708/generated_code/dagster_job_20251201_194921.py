from dagster import op, job, resource, Field, String, graph, In, Nothing
import snowflake.connector
from slack_sdk import WebClient
from typing import Dict, Any


@resource(config_schema={
    "account": Field(String),
    "user": Field(String),
    "password": Field(String),
    "warehouse": Field(String),
    "database": Field(String),
    "schema": Field(String),
})
def snowflake_resource(context):
    """Resource for managing Snowflake connections."""
    conn = snowflake.connector.connect(
        account=context.resource_config["account"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        warehouse=context.resource_config["warehouse"],
        database=context.resource_config["database"],
        schema=context.resource_config["schema"],
    )
    try:
        yield conn
    finally:
        conn.close()


@resource(config_schema={
    "token": Field(String),
    "channel": Field(String),
})
def slack_resource(context):
    """Resource for sending Slack notifications."""
    client = WebClient(token=context.resource_config["token"])
    yield {
        "client": client,
        "channel": context.resource_config["channel"],
    }


@op(
    required_resource_keys={"snowflake", "slack"},
    config_schema={
        "schema_name": Field(String),
        "table_name": Field(String),
        "sql_query": Field(String),
    },
    ins={"upstream": In(Nothing)},
)
def process_table(context, upstream) -> None:
    """
    Process a single table through the ELT pipeline:
    1. Create temp table via CTAS
    2. Validate it contains data
    3. Ensure target table exists
    4. Swap temp table with target
    """
    config = context.op_config
    schema = config["schema_name"]
    table = config["table_name"]
    sql = config["sql_query"]
    
    temp_table = f"{table}_TEMP"
    full_temp_table = f"{schema}.{temp_table}"
    full_table = f"{schema}.{table}"
    
    snowflake_conn = context.resources.snowflake
    slack = context.resources.slack
    
    try:
        # Step 1: Create temp table with CTAS
        context.log.info(f"Creating temp table {full_temp_table}")
        create_sql = f"CREATE OR REPLACE TABLE {full_temp_table} AS {sql}"
        with snowflake_conn.cursor() as cur:
            cur.execute(create_sql)
        
        # Step 2: Validate temp table has at least one record
        context.log.info(f"Validating temp table {full_temp_table} has data")
        count_sql = f"SELECT COUNT(*) FROM {full_temp_table}"
        with snowflake_conn.cursor() as cur:
            cur.execute(count_sql)
            row_count = cur.fetchone()[0]
            
        if row_count == 0:
            raise ValueError(f"Temp table {full_temp_table} contains no records")
        
        # Step 3: Ensure target table exists
        context.log.info(f"Ensuring target table {full_table} exists")
        ensure_table_sql = f"CREATE TABLE IF NOT EXISTS {full_table} LIKE {full_temp_table}"
        with snowflake_conn.cursor() as cur:
            cur.execute(ensure_table_sql)
        
        # Step 4: Swap temp table with target table
        context.log.info(f"Swapping {full_temp_table} with {full_table}")
        swap_sql = f"ALTER TABLE {full_table} SWAP WITH {full_temp_table}"
        with snowflake_conn.cursor() as cur:
            cur.execute(swap_sql)
        
        # Clean up temp table
        drop_sql = f"DROP TABLE IF EXISTS {full_temp_table}"
        with snowflake_conn.cursor() as cur:
            cur.execute(drop_sql)