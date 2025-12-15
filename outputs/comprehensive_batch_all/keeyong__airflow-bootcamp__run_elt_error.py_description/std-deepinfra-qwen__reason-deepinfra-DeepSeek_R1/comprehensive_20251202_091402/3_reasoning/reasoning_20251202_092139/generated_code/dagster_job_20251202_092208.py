<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline is a daily ELT process using Snowflake, with sequential processing of tables defined in a configuration. Each table task involves creating a temp table, validating it, ensuring the target exists, swapping tables, and handling failures with Slack notifications.

First, I'll outline the main components. Each step in the table processing should be an op. Since the tasks are sequential, each op for a table will depend on the previous one. But since the number of tables is dynamic, I can't hardcode the ops. So maybe I need to use a graph or a factory pattern to generate the ops dynamically.

Wait, Dagster doesn't support dynamic op generation directly. So perhaps I can loop through the TABLES configuration and create a chain of ops where each depends on the previous. That would require defining a function that generates the ops in sequence.

Alternatively, using a graph with a list of ops where each depends on the prior. But how to handle a variable number of tables? Maybe using a for loop to build the dependencies.

Let me think. The user mentioned that the pipeline processes tables sequentially in a linear chain, with each task depending on the previous. So for each table in the config, there's an op, and each op depends on the previous one.

So, first, I need a config that lists the tables. The config should have schema, table_name, and sql. Then, for each table in the config, create an op that does the four steps. But since each table's processing is similar, maybe create a function that returns an op for a given table config.

But in Dagster, each op must be defined with a unique name. So, for each table, generate an op with a unique name, like process_table_{table_name}. Then, chain them in order.

But how to generate these ops dynamically? Using a loop over the TABLES configuration, creating an op for each, and setting the dependencies.

Wait, perhaps using a job with a list of ops, where each op is connected to the next. For example, if there are three tables, the job would have process_table_1 -> process_table_2 -> process_table_3.

So, the approach is:

1. Define a list of tables in the config (maybe as a resource or config schema).
2. For each table in the list, create an op (using a factory function).
3. Chain the ops in sequence.

But in Dagster, you can't dynamically create ops at runtime. They need to be defined statically. So, unless the number of tables is fixed, this might be tricky.

Wait, the user's example says "TABLES configuration", which is probably a list. So perhaps the job is designed to process all tables in the config, but each as a separate op in sequence.

Alternatively, maybe the job has a single op that processes all tables in sequence. But that would not allow Dagster to track each step individually. The user wants each table processing as a separate task, so each should be an op.

Hmm. So perhaps the solution is to have a configurable list of tables, and for each table, generate an op. But since the number can vary, the user might need to adjust the code each time the tables change, which isn't ideal. Alternatively, use a configurable list and generate the ops dynamically based on that.

But in Dagster, you can't create ops dynamically in the job. So maybe the job is designed with a fixed set of tables, but that's not flexible. Alternatively, the job could have a single op that processes all tables in sequence, handling each in a loop within the op. But then, in the Dagster UI, you wouldn't see each table's processing as a separate step.

The user's pipeline description says "each table processing task depends on completion of the previous table task". So each table is a separate task (op) with a linear dependency.

Given that, perhaps the example uses a fixed set of tables for simplicity, like three tables, and chains their ops. But in reality, the user would have to generate the ops based on their config.

But the problem says to provide a minimal example. So maybe the code assumes a fixed list of tables, say, three, and creates ops for each, chained in sequence.

Alternatively, use a configurable list and a single op that processes them in sequence. But that would not model each table as a separate step.

The user's constraints mention that tasks must have explicit dependencies reflecting the order. So each table's op must depend on the prior. Therefore, the code must have a way to generate a sequence of ops dynamically.

Wait, perhaps using a graph with a loop that builds the dependencies. But in Dagster, the graph is defined statically. So perhaps the example uses a fixed number of tables, say, three, and manually chains them. For the purpose of the example, that's acceptable.

Looking at the sample answer, the user provided code that has a list of tables in the config, and a function that creates an op for each table, then chains them in the job.

Wait, in the sample answer, the process_table op is defined once, but how does it handle multiple tables? Oh, no, in the sample answer, they have a list of tables in the config, and for each table, they create a dynamic op by using a function that generates the op with a unique name.

Wait, the code uses a function called make_table_op which takes a table config and returns an op. Then, for each table in the TABLES_CONFIG, they create an op and collect them in a list. Then, in the job definition, they set the dependencies by iterating through the list and having each op depend on the previous one.

Yes, that's the approach. So the code defines a helper function make_table_op that generates an op for a given table config. Then, for each table in the TABLES_CONFIG list, it creates an op. Then, in the job, the dependencies are set by looping through the ops and connecting each to the next.

So the key parts are:

- A config with the tables.
- A function to generate an op per table.
- Collecting the ops in order.
- Setting up the dependencies between them.

Additionally, the Snowflake resource is stubbed, and the Slack resource is handled via a failure hook.

The code also includes a minimal example of running the job in-process.

So, putting it all together:

- Define the table config as a list of dictionaries.
- Create a make_table_op function that returns an op for a table.
- Generate a list of ops by applying make_table_op to each table in the config.
- Define the job with these ops, setting each to depend on the previous.
- Add resources for Snowflake and Slack, with stubbed implementations.
- Include a failure hook for Slack notifications.
- Add the main block to execute the job.

That's the approach taken in the sample answer. The code uses a loop to generate the ops and set their dependencies, allowing for any number of tables in the config. Each op is named uniquely based on the table name. The resources are provided as stubs, and the failure hook sends a Slack message on op failure.

The code is PEP 8 compliant, uses explicit dependencies, and includes minimal docstrings. The execute_in_process is included for local execution.
</think>

from dagster import job, op, OpExecutionContext, Failure, ResourceParam, HookDefinition, hook

# Example configuration - would typically come from a YAML file or environment
TABLES_CONFIG = [
    {
        "schema": "analytics",
        "table_name": "user_activity",
        "sql": "SELECT * FROM raw.user_activity WHERE date = CURRENT_DATE()"
    },
    {
        "schema": "analytics",
        "table_name": "sales_daily",
        "sql": "SELECT product_id, SUM(sales) FROM raw.sales GROUP BY 1"
    }
]

# Stub Snowflake resource
class SnowflakeConnection:
    def execute_query(self, query: str):
        print(f"Executing Snowflake query: {query}")
        return {"rowcount": 1}  # Simulated result

    def table_exists(self, schema: str, table: str) -> bool:
        return False  # Simplification for example

# Simplified resources
@hook(required_resource_keys={"slack"})
def slack_failure_notification(context: OpExecutionContext):
    context.resources.slack.send_message(
        f"Operation {context.op.name} failed: {context.op_exception}"
    )

def make_table_op(table_config: dict):
    @op(
        name=f"process_{table_config['table_name']}",
        description=f"Process {table_config['schema']}.{table_config['table_name']}",
    )
    def table_op(context: OpExecutionContext, previous_step: None):
        snowflake = context.resources.snowflake
        temp_table = f"{table_config['table_name']}_temp"

        # Create temporary table
        create_sql = f"CREATE TEMPORARY TABLE {temp_table} AS {table_config['sql']}"
        result = snowflake.execute_query(create_sql)
        
        # Validate records
        if result["rowcount"] == 0:
            raise Failure(f"No records created for {table_config['table_name']}")
        
        # Ensure target exists
        if not snowflake.table_exists(table_config["schema"], table_config["table_name"]):
            snowflake.execute_query(
                f"CREATE TABLE {table_config['schema']}.{table_config['table_name']} LIKE {temp_table}"
            )
        
        # Swap tables
        snowflake.execute_query(
            f"ALTER TABLE {table_config['schema']}.{table_config['table_name']} "
            f"SWAP WITH {temp_table}"
        )
        
        context.log.info(f"Processed {table_config['schema']}.{table_config['table_name']}")

    return table_op

@job(
    resource_defs={
        "snowflake": ResourceParam(SnowflakeConnection()),
        "slack": ResourceParam(str)  # Would be Slack client in real implementation
    },
    hooks={HookDefinition(slack_failure_notification)},
)
def daily_elt_job():
    ops = [make_table_op(table) for table in TABLES_CONFIG]
    for i in range(1, len(ops)):
        ops[i](ops[i-1]())

if __name__ == "__main__":
    result = daily_elt_job.execute_in_process()