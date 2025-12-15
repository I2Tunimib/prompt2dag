from dagster import op, job, resource, RetryPolicy, Failure, Field, String, In, Out

# Simplified resource stubs
@resource(config_schema={"snowflake_account": Field(String), "snowflake_user": Field(String)})
def snowflake_resource(context):
    return {
        "account": context.resource_config["snowflake_account"],
        "user": context.resource_config["snowflake_user"],
    }

@resource(config_schema={"slack_webhook_url": Field(String)})
def slack_resource(context):
    return context.resource_config["slack_webhook_url"]

# Helper functions
def create_temp_table(snowflake_conn, table_config):
    # Execute CTAS statement to create temporary table
    sql = table_config["sql"]
    temp_table_name = f"{table_config['schema']}.temp_{table_config['table']}"
    snowflake_conn.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS ({sql})")

def validate_temp_table(snowflake_conn, table_config):
    # Validate the temporary table contains at least one record
    temp_table_name = f"{table_config['schema']}.temp_{table_config['table']}"
    result = snowflake_conn.execute(f"SELECT COUNT(*) FROM {temp_table_name}").fetchone()
    if result[0] == 0:
        raise Failure(f"Temporary table {temp_table_name} is empty")

def ensure_target_table_exists(snowflake_conn, table_config):
    # Ensure the target table exists by creating an empty version if missing
    target_table_name = f"{table_config['schema']}.{table_config['table']}"
    snowflake_conn.execute(f"CREATE TABLE IF NOT EXISTS {target_table_name} LIKE {target_table_name}")

def swap_tables(snowflake_conn, table_config):
    # Swap the temporary table with the target table
    temp_table_name = f"{table_config['schema']}.temp_{table_config['table']}"
    target_table_name = f"{table_config['schema']}.{table_config['table']}"
    snowflake_conn.execute(f"ALTER TABLE {temp_table_name} SWAP WITH {target_table_name}")

def send_slack_notification(slack_webhook_url, message):
    # Send a Slack notification
    import requests
    requests.post(slack_webhook_url, json={"text": message})

# Ops
@op(required_resource_keys={"snowflake", "slack"})
def process_table(context, table_config):
    snowflake_conn = context.resources.snowflake
    slack_webhook_url = context.resources.slack

    try:
        create_temp_table(snowflake_conn, table_config)
        validate_temp_table(snowflake_conn, table_config)
        ensure_target_table_exists(snowflake_conn, table_config)
        swap_tables(snowflake_conn, table_config)
    except Exception as e:
        send_slack_notification(slack_webhook_url, f"Task failed for table {table_config['table']}: {str(e)}")
        raise

# Job
@job(
    resource_defs={
        "snowflake": snowflake_resource,
        "slack": slack_resource,
    },
    retry_policy=RetryPolicy(max_retries=3),
)
def daily_elt_pipeline():
    TABLES = [
        {"schema": "public", "table": "table1", "sql": "SELECT * FROM raw_table1"},
        {"schema": "public", "table": "table2", "sql": "SELECT * FROM raw_table2"},
        {"schema": "public", "table": "table3", "sql": "SELECT * FROM raw_table3"},
    ]

    previous_op = None
    for table_config in TABLES:
        current_op = process_table.alias(f"process_{table_config['table']}")(table_config)
        if previous_op:
            current_op = previous_op >> current_op
        previous_op = current_op

if __name__ == "__main__":
    result = daily_elt_pipeline.execute_in_process(
        run_config={
            "resources": {
                "snowflake": {
                    "config": {
                        "snowflake_account": "your_snowflake_account",
                        "snowflake_user": "your_snowflake_user",
                    }
                },
                "slack": {
                    "config": {
                        "slack_webhook_url": "your_slack_webhook_url",
                    }
                },
            }
        }
    )