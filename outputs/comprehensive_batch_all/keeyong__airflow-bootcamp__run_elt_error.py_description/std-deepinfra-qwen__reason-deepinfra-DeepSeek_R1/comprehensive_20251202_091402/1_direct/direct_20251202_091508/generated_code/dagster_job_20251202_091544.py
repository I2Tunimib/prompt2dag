from dagster import op, job, resource, RetryPolicy, Failure, Field, String, In, Out
from dagster_slack import SlackResource

# Simplified Snowflake resource
@resource(config_schema={"user": Field(String), "password": Field(String), "account": Field(String)})
def snowflake_resource(context):
    from snowflake.connector import connect
    conn = connect(
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        account=context.resource_config["account"]
    )
    return conn

# Simplified Slack resource
@resource(config_schema={"token": Field(String)})
def slack_resource(context):
    return SlackResource(context.resource_config["token"])

# Configuration for table processing
TABLES = [
    {"schema": "public", "table": "table1", "sql": "SELECT * FROM raw_table1"},
    {"schema": "public", "table": "table2", "sql": "SELECT * FROM raw_table2"},
    {"schema": "public", "table": "table3", "sql": "SELECT * FROM raw_table3"}
]

# Helper function to execute SQL
def execute_sql(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()

# Helper function to check if table exists
def table_exists(conn, schema, table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'")
    result = cursor.fetchone()[0]
    cursor.close()
    return result > 0

# Helper function to create empty table
def create_empty_table(conn, schema, table):
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} (LIKE {schema}.{table} INCLUDING ALL)")
    cursor.close()

# Helper function to send Slack message
def send_slack_message(slack, message):
    slack.chat.post_message(channel="#alerts", text=message)

# Op to process a single table
@op(required_resource_keys={"snowflake", "slack"})
def process_table(context, table_config, prev_table_processed=None):
    conn = context.resources.snowflake
    slack = context.resources.slack
    schema = table_config["schema"]
    table = table_config["table"]
    sql = table_config["sql"]

    try:
        # Create temporary table using CTAS
        temp_table = f"{table}_temp"
        execute_sql(conn, f"CREATE OR REPLACE TEMPORARY TABLE {schema}.{temp_table} AS ({sql})")

        # Validate temporary table contains at least one record
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{temp_table}")
        count = cursor.fetchone()[0]
        cursor.close()
        if count == 0:
            raise ValueError(f"Temporary table {schema}.{temp_table} is empty")

        # Ensure target table exists
        if not table_exists(conn, schema, table):
            create_empty_table(conn, schema, table)

        # Swap temporary table with target table
        execute_sql(conn, f"ALTER TABLE {schema}.{temp_table} SWAP WITH {schema}.{table}")

    except Exception as e:
        send_slack_message(slack, f"Error processing table {schema}.{table}: {str(e)}")
        raise Failure(f"Error processing table {schema}.{table}: {str(e)}")

# Job to orchestrate the pipeline
@job(resource_defs={"snowflake": snowflake_resource, "slack": slack_resource})
def daily_elt_pipeline():
    prev_table_processed = None
    for table_config in TABLES:
        prev_table_processed = process_table(prev_table_processed, table_config)

# Launch pattern
if __name__ == '__main__':
    result = daily_elt_pipeline.execute_in_process(
        run_config={
            "resources": {
                "snowflake": {
                    "config": {
                        "user": "your_snowflake_user",
                        "password": "your_snowflake_password",
                        "account": "your_snowflake_account"
                    }
                },
                "slack": {
                    "config": {
                        "token": "your_slack_token"
                    }
                }
            }
        }
    )