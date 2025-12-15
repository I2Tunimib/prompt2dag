from dagster import op, job, resource, RetryPolicy, Failure, Field, String, In, Out
from dagster_slack import SlackResource

# Simplified Snowflake resource
@resource(config_schema={"user": Field(String), "password": Field(String), "account": Field(String)})
def snowflake_resource(context):
    from snowflake.connector import connect
    conn = connect(
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        account=context.resource_config["account"],
    )
    return conn

# Simplified Slack resource
@resource
def slack_resource(context):
    return SlackResource(token="your-slack-token")

# Configuration for table processing
TABLES = [
    {"schema": "public", "table": "table1", "sql": "SELECT * FROM raw_table1"},
    {"schema": "public", "table": "table2", "sql": "SELECT * FROM raw_table2"},
    {"schema": "public", "table": "table3", "sql": "SELECT * FROM raw_table3"},
]

# Helper function to execute SQL
def execute_sql(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()

# Helper function to check if table is empty
def table_has_records(conn, schema, table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

# Helper function to create empty table
def create_empty_table(conn, schema, table):
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} (id INT)")
    cursor.close()

# Op to process a single table
@op(required_resource_keys={"snowflake", "slack"})
def process_table(context, table_config, previous_table_processed=None):
    conn = context.resources.snowflake
    schema = table_config["schema"]
    table = table_config["table"]
    sql = table_config["sql"]

    # Create temporary table
    temp_table = f"{table}_temp"
    execute_sql(conn, f"CREATE OR REPLACE TEMPORARY TABLE {schema}.{temp_table} AS ({sql})")

    # Validate temporary table
    if not table_has_records(conn, schema, temp_table):
        context.log.error(f"Temporary table {schema}.{temp_table} is empty")
        context.resources.slack.chat.post_message(
            channel="#alerts",
            text=f"Error: Temporary table {schema}.{temp_table} is empty"
        )
        raise Failure(f"Temporary table {schema}.{temp_table} is empty")

    # Ensure target table exists
    create_empty_table(conn, schema, table)

    # Swap temporary table with target table
    execute_sql(conn, f"ALTER TABLE {schema}.{temp_table} SWAP WITH {schema}.{table}")

    context.log.info(f"Processed table {schema}.{table}")

# Job to orchestrate the pipeline
@job(
    resource_defs={
        "snowflake": snowflake_resource,
        "slack": slack_resource,
    },
    config={
        "resources": {
            "snowflake": {
                "config": {
                    "user": "your-snowflake-user",
                    "password": "your-snowflake-password",
                    "account": "your-snowflake-account",
                }
            },
            "slack": {
                "config": {
                    "token": "your-slack-token",
                }
            },
        }
    },
)
def daily_elt_pipeline():
    previous_op = None
    for table_config in TABLES:
        op = process_table.alias(f"process_{table_config['table']}").with_retry_policy(RetryPolicy(max_retries=3))
        if previous_op:
            op(previous_op)
        else:
            op()
        previous_op = op

if __name__ == "__main__":
    result = daily_elt_pipeline.execute_in_process()