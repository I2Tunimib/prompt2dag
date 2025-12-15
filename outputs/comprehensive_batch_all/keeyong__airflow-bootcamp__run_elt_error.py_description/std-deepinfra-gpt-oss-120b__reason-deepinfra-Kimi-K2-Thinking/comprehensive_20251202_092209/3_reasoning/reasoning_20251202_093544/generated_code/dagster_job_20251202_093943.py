from dagster import op, job, In, Nothing, ResourceDefinition, OpExecutionContext, Config
from typing import Any, Dict, List


# Configuration for tables - in a real scenario this would be loaded from a file or environment
TABLES = [
    {
        "schema": "analytics",
        "table_name": "user_metrics",
        "sql": "SELECT user_id, COUNT(*) as order_count FROM raw.orders GROUP BY user_id"
    },
    {
        "schema": "analytics",
        "table_name": "order_metrics",
        "sql": "SELECT DATE(order_date) as order_day, COUNT(*) as daily_orders FROM raw.orders GROUP BY order_day"
    },
    {
        "schema": "analytics",
        "table_name": "product_metrics",
        "sql": "SELECT product_id, SUM(quantity) as total_sold FROM raw.order_items GROUP BY product_id"
    }
]


# Stub resource for Snowflake operations
class SnowflakeResource:
    """Stub Snowflake resource for demonstration."""
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query against Snowflake."""
        print(f"Executing Snowflake query: {query}")
        # Simulate query execution
        if "COUNT(*)" in query:
            return [{"cnt": 100}]  # Simulate non-empty table
        return []


# Stub resource for Slack notifications
class SlackResource:
    """Stub Slack resource for demonstration."""
    
    def send_notification(self, message: str) -> None:
        """Send a notification to Slack."""
        print(f"Slack notification: {message}")


# Resource definitions
@ResourceDefinition
def snowflake_resource(_context):
    """Snowflake resource definition."""
    return SnowflakeResource()


@ResourceDefinition
def slack_resource(_context):
    """Slack resource definition."""
    return SlackResource()


# Op to process a single table
@op(
    ins={"previous": In(Nothing)},
    config_schema={
        "schema": str,
        "table_name": str,
        "sql": str
    },
    required_resource_keys={"snowflake", "slack"}
)
def process_table(context: OpExecutionContext, previous: None) -> None:
    """
    Process a single table through the ELT pipeline:
    1. Create temp table with CTAS
    2. Validate temp table has records
    3. Ensure target table exists
    4. Swap temp table with target table
    """
    config = context.op_config
    schema = config["schema"]
    table_name = config["table_name"]
    sql = config["sql"]
    
    snowflake = context.resources.snowflake
    slack = context.resources.slack
    
    temp_table = f"{schema}.temp_{table_name}"
    target_table = f"{schema}.{table_name}"
    
    try:
        # Step 1: Create temp table with CTAS
        context.log.info(f"Creating temp table: {temp_table}")
        snowflake.execute_query(f"CREATE OR REPLACE TABLE {temp_table} AS {sql}")
        
        # Step 2: Validate temp table contains at least one record
        context.log.info(f"Validating temp table: {temp_table}")
        result = snowflake.execute_query(f"SELECT COUNT(*) as cnt FROM {temp_table}")
        count = result[0]["cnt"] if result else 0
        
        if count == 0:
            raise ValueError(f"Temp table {temp_table} has no records")
        
        # Step 3: Ensure target table exists
        context.log.info(f"Ensuring target table exists: {target_table}")
        snowflake.execute_query(
            f"CREATE TABLE IF NOT EXISTS {target_table} LIKE {temp_table}"
        )
        
        # Step 4: Swap temp table with target table
        context.log.info(f"Swapping temp table with target: {temp_table} <-> {target_table}")
        snowflake.execute_query(f"ALTER TABLE {temp_table} SWAP WITH {target_table}")
        
        context.log.info(f"Successfully processed table: {target_table}")
        
    except Exception as e:
        error_msg = f"Failed to process table {target_table}: {str(e)}"
        context.log.error(error_msg)
        slack.send_notification(error_msg)
        raise


# Job definition
@job(
    resource_defs={
        "snowflake": snowflake_resource,
        "slack": slack_resource
    }
)
def daily_elt_job():
    """
    Daily ELT job that processes tables sequentially.
    Each table processing task depends on the completion of the previous one.
    """
    # Create aliased op definitions for each table
    process_user_metrics = process_table.alias("process_user_metrics")
    process_order_metrics = process_table.alias("process_order_metrics")
    process_product_metrics = process_table.alias("process_product_metrics")
    
    # Create invocations and wire them sequentially
    table1 = process_user_metrics()
    table2 = process_order_metrics(previous=table1)
    table3 = process_product_metrics(previous=table2)
    
    # The chain is: table1 -> table2 -> table3


# Launch pattern
if __name__ == "__main__":
    # Execute the job with the default configuration
    result = daily_elt_job.execute_in_process(
        run_config={
            "ops": {
                "process_user_metrics": {
                    "config": TABLES[0]
                },
                "process_order_metrics": {
                    "config": TABLES[1]
                },
                "process_product_metrics": {
                    "config": TABLES[2]
                }
            }
        }
    )
    
    if result.success:
        print("Pipeline execution completed successfully")
    else:
        print("Pipeline execution failed")