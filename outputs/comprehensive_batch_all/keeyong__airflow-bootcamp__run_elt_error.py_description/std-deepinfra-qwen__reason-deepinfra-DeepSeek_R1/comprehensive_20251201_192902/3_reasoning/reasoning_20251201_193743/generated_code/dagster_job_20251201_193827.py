<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline processes tables sequentially, each with the same steps: create temp table, validate data, ensure target exists, swap tables. Each task depends on the previous one.

First, I'll define each step as Dagster ops. Since each table task is identical but runs in sequence, I can create a reusable op for processing a single table. But since the tasks are sequential per table, maybe each table's processing is a separate op that depends on the previous. Wait, the description says "each table processing task depends on completion of the previous table task". So if there are multiple tables in the config, each subsequent op depends on the prior one.

But how to handle variable numbers of tables? Since Dagster jobs are static, I need to loop through the tables and generate a chain of ops. But since the user provided code must be executable, perhaps the example uses a fixed list of tables, like two tables, and manually sets dependencies. Alternatively, use a configurable list.

Wait, the user mentioned "TABLES configuration" in the pipeline steps. So maybe the job is configured with a list of tables, each with their schema, name, and SQL. So the job should take a config that specifies the tables. But in Dagster, you can't dynamically generate ops based on config at runtime. So perhaps the tables are defined in the config, and the job processes them in sequence using a fixed set of ops, but that's not possible if the number varies. Hmm.

Alternatively, the user might expect that the code is written to handle a list of tables defined in the config, but since Dagster doesn't allow dynamic op generation, perhaps the example uses a predefined list, like two tables, and chains their ops manually. Or maybe use a single op that processes all tables in sequence, but that's not using Dagster's dependency system.

Wait, the pipeline steps mention "linear sequence of identical tasks, one for each table". So each table is processed by a task, and each task depends on the previous. So for example, if there are three tables, the ops would be process_table1 -> process_table2 -> process_table3.

But how to create this dynamically? Since Dagster jobs are static, perhaps the example uses a fixed number of tables, like two, and manually sets the dependencies. Or maybe the code uses a function to generate the ops and dependencies based on a configurable list. But the user said to return executable code, so perhaps the example uses a fixed list of tables in the config, and the job is structured with a loop that creates the ops and sets up the dependencies.

Wait, but in Dagster, you can't dynamically create ops in a job based on a config. The job structure is static. So the approach would be to have a single op that processes all tables in sequence. But that doesn't fit the requirement of modeling each table task as an op. Alternatively, perhaps the job is written with a fixed number of tables, but that's not scalable. The user might expect a solution that uses a configurable list and processes each table in sequence using a single op that's called multiple times with different config, but that's not possible with Dagster's op dependencies unless the ops are separate.

Alternatively, perhaps the job uses a graph that, for each table in the config, creates a chain of ops. But again, since the config can vary, this isn't possible unless the code is generated dynamically. But the user wants executable code, so maybe the example uses a fixed set of tables, say two, and manually creates the dependencies between their processing ops.

Let me proceed with that approach. The code will have a config schema that includes a list of tables. Each table has schema, name, and sql. Then, for each table in the config, create an op. But since the config is provided at runtime, and the job structure is static, this isn't feasible. Therefore, perhaps the example uses a fixed set of tables, like two, and the job is structured with two ops, each processing a table, with the second depending on the first.

But the user's pipeline processes tables sequentially, one after another, each as a separate task. So for each table in the config, there's an op, and each op depends on the previous. Since Dagster doesn't support dynamic op generation, the example will have to hardcode a few tables, but show the pattern.

Alternatively, use a single op that processes all tables in sequence, but that doesn't model each table as a separate op. The user's pipeline requires each table task to be a separate step, so perhaps the example uses a fixed number of tables and manually sets dependencies.

Another approach: create a function that generates the ops for each table and connects them in sequence. But in Dagster, the job is defined statically. So perhaps the code uses a list of table names and loops through them to create ops and set dependencies. But how?

Wait, maybe using a job that's built programmatically. For example, if the tables are known at the time of job definition, you can create a list of ops and chain them together. Let's say the tables are defined in a config variable, like TABLES = ['table1', 'table2', 'table3'], and then the job is built by creating an op for each table and setting each to depend on the previous.

But in the code, the user would need to define each op as a separate function, but that's not feasible if the number of tables is variable. Alternatively, use a factory function to generate the ops. For example, a function that returns an op for processing a table, and then create multiple instances of that op for each table.

Yes, that's possible. So, create a function that generates an op for a specific table, then create instances for each table and chain them together.

But the user's code must be executable. Let's outline the code structure.

First, define the config for tables. Let's say the config is a list of tables with their details. Then, create a function that takes a table config and returns an op. Then, in the job, create an op for each table and set dependencies.

But how to do this in Dagster? Let's see.

In Dagster, you can use a decorator to define ops, but if you need to create multiple similar ops, you can use a factory function. For example:

def make_table_op(table_name):
    @op(name=f"process_{table_name}")
    def _op(context):
        # process the table
    return _op

Then, for each table in the config, create an op using this factory and add them to the job.

But the problem is that the job must be defined statically. So if the tables are known at the time of writing the code, you can do this. For example, if the tables are ['users', 'orders'], then create two ops and chain them.

But the user's pipeline is configuration-driven, so perhaps the example uses a config with a list of tables, and the code is written to handle any number of tables by generating ops dynamically. However, Dagster doesn't support dynamic op generation in a job. Therefore, the example must use a fixed number of tables, but show the pattern.

Alternatively, the code can have a single op that processes all tables in sequence, using a loop inside the op. But that would not model each table as a separate op, which the user's pipeline requires.

Hmm. The user's pipeline steps state that each table processing task is a separate step, so each should be a separate op. Therefore, the example must have multiple ops, each for a table, connected in sequence.

Given that, perhaps the example uses two tables as a sample, with their ops named process_table1 and process_table2, and the second depends on the first. Then, the job's dependencies are set accordingly.

But how to make this configurable? The user's pipeline is configuration-driven, so the code should read the tables from the config. But since the job structure is static, the code can't dynamically create ops based on the config. Therefore, the example will have a fixed set of tables, but the code structure allows for adding more by modifying the config and the job definition.

Wait, perhaps the code uses a graph that takes the list of tables from the config and processes them in sequence. But again, the job's structure is static. So maybe the code uses a single op that processes all tables in sequence, handling each table one by one within the op. That way, the op's logic loops through the tables in the config and processes them in order. But this approach doesn't model each table as a separate op, which the user's pipeline requires.

The user's pipeline requires each table task to be a separate step (op) with dependencies. Therefore, the example must have separate ops for each table. Since the number of tables can vary, the code must be written in a way that allows adding more ops by modifying the code, not the config. But the user's pipeline is configuration-driven, so perhaps the example uses a fixed number of tables but demonstrates the pattern.

Alternatively, perhaps the code uses a single op that is configured with a table name and processes that table. Then, the job is built by creating multiple instances of this op, each configured for a different table, and setting dependencies between them. This is possible in Dagster using the `configured` method.

Yes! That's the solution. So, create a generic op that processes a single table, then create configured instances of this op for each table in the config, and chain them together in the job.

For example:

- Define a generic op `process_table` that takes a `table_config` as input.
- Use `configured` to create specific instances of this op for each table, each with their own configuration.
- Then, in the job, set the dependencies between these configured ops.

But how to pass the output of one op to the next? Since each op doesn't produce an output that the next op needs (they are sequential but not passing data), the dependencies are just for execution order. So each configured op depends on the previous one.

So, the code would look like this:

1. Define a Snowflake resource.
2. Define a generic process_table op that uses the Snowflake resource and the table's config.
3. Configure multiple instances of this op for each table.
4. Create a job that connects these ops in sequence.

But how to handle variable numbers of tables? Since the code must be executable, perhaps the example uses two tables and shows how to chain them. The user can extend it by adding more configured ops.

Wait, but using `configured` allows creating multiple instances of the same op with different configurations. So, for example:

process_users = process_table.configured({"table_name": "users", ...}, name="process_users")
process_orders = process_table.configured({"table_name": "orders", ...}, name="process_orders")

Then, in the job, process_orders depends on process_users.

But the configuration for the tables would need to be part of the job definition, not the run config. Alternatively, the job can take a list of tables as config, but then dynamically generating the ops isn't possible.

Hmm. This is a bit tricky. Let's think again.

The user's pipeline processes tables sequentially, each as a separate task. Each task has the same steps but for a different table. The tasks must run in order, each depending on the previous.

In Dagster, to model this, each table's processing is an op, and the ops are connected in a linear chain. The challenge is to create these ops dynamically based on a configurable list of tables.

Since Dagster doesn't support dynamic generation of ops based on run-time config, the solution must either:

1. Use a fixed set of tables, with each op defined explicitly in the code, and dependencies set manually. This is feasible for an example with two tables.

2. Use a single op that processes all tables in sequence, looping through them internally. This doesn't model each table as a separate op but meets the execution order requirement.

The user's pipeline requires each table to be a separate task (op), so option 1 is better for the example.

Therefore, the code will have two example tables (e.g., 'users' and 'orders') each processed by their own op, with the second depending on the first. The code will demonstrate the pattern, which can be extended by adding more ops for additional tables.

Now, let's outline the code structure.

First, define the Snowflake resource. The user mentioned Snowflake integration, so we'll create a resource. The resource can be a simple class or a dictionary with a connection.

Then, define the Slack resource for notifications on failure. Since failure notifications are required, we can use Dagster's event handling. However, the user mentioned "Slack integration for failure notifications via callback", so perhaps the op checks for failures and sends a message. Alternatively, use Dagster's hooks or event handlers. But for simplicity, perhaps each op has a try-except block, and on failure, calls a Slack resource to send a message.

But the user's pipeline says "sends Slack notifications on task failures via callback". So perhaps using Dagster's failure hooks.

But the code needs to be minimal. So perhaps the Slack resource is a mock, and in the op, on failure, it sends a message.

Alternatively, use Dagster's built-in event handling. For example, using a failure hook. But the code example can include a resource for Slack, which is passed to the ops, and in case of exception, the op sends a message via the Slack resource.

Now, the steps for each table processing op:

1. Create temp table with CTAS.
2. Validate temp table has at least one record.
3. Ensure target table exists (create if missing).
4. Swap temp and target tables.

Each of these steps would be part of the op's logic.

The op will use the Snowflake resource to execute SQL commands.

Now, putting it all together:

- Define resources: snowflake and slack.
- Define the process_table op, which takes the table config as input.
- The op uses the Snowflake resource to execute SQL.
- The op has a try-except block to handle failures and send Slack messages.
- Configure multiple instances of this op for each table.
- Chain them in the job.

But again, since the job is static, the example will have two tables.

Wait, but using the configured approach, perhaps the code can be written as follows:

Define a generic op that processes a table, taking the table config as a parameter. Then, create configured instances of this op for each table in the config.

But how to pass the table config? The op's config can include the table details. So each configured op has its own config.

For example:

@op(required_resource_keys={'snowflake', 'slack'})
def process_table(context):
    table_config = context.op_config
    # process the table using table_config

Then, configure this op for each table.

But in the job, how to chain them? Each configured op would need to depend on the previous. So for example:

process_users = process_table.configured({'table_name': 'users', ...}, name='process_users')
process_orders = process_table.configured({'table_name': 'orders', ...}, name='process_orders')

@job(resource_defs={
    'snowflake': snowflake_resource,
    'slack': slack_resource
})
def elt_job():
    process_orders(process_users())

But this requires that each configured op is a separate node in the job, and their dependencies are set in sequence.

Yes, this approach works. So the code would define the generic process_table op, then create configured instances for each table, and in the job definition, call them in order, passing the output (even if there's no output, just for dependencies).

But since the ops don't produce outputs, perhaps using the Dagster syntax where each subsequent op depends on the previous by specifying them as inputs. However, in Dagster, if an op doesn't have an output, you can still have dependencies by using the start= argument or by using the .alias() method. Wait, no. Each op's invocation in the job defines the dependencies. For example:

@job
def my_job():
    a = op_a()
    b = op_b(a)
    c = op_c(b)

So even if op_a doesn't produce an output, op_b depends on op_a's completion.

Therefore, in the example, each configured op can be called in sequence, with each depending on the previous.

So, the code would look like:

@job(resource_defs={...})
def elt_job():
    table_ops = []
    tables = [{'name': 'table1', ...}, {'name': 'table2', ...}]
    prev_op = None
    for table in tables:
        current_op = process_table.configured(table, name=f"process_{table['name']}")
        if prev_op is not None:
            current_op(prev_op())
        else:
            current_op()
        prev_op = current_op

Wait, but this is not valid Python code because the job is defined statically. You can't loop inside the job function to create ops dynamically. Therefore, this approach isn't feasible.

Thus, the only way is to manually define each configured op and their dependencies in the job. So for a sample with two tables, the code would have:

@job(resource_defs={...})
def elt_job():
    process_table1 = process_table.configured(table1_config, name="process_table1")
    process_table2 = process_table.configured(table2_config, name="process_table2")
    process_table2(process_table1())

But how to get the table configs? They can be hardcoded in the job, or passed via the job's config. But since the job's structure is static, the tables must be known at the time of job definition.

Alternatively, the job's config includes a list of tables, and the process_table op processes all tables in sequence within a single op. But that doesn't model each table as a separate op.

Given the constraints, the example will use two tables, manually defined, with their ops configured and dependencies set.

Now, putting it all together:

- Define resources for Snowflake and Slack.
- Define the process_table op, which takes the table config from op config.
- Configure two instances of this op for two tables.
- Define the job with these ops and their dependencies.

Additionally, handle failure notifications in the op. So within the op, use try-except blocks, and on failure, call the Slack resource to send a message.

Now, code:

First, define the resources. For Snowflake, a simple resource that has a execute_sql method. For Slack, a resource with a send_message method.

Then, the process_table op:

@op(required_resource_keys={'snowflake', 'slack'}, 
     config_schema={
         'schema': str,
         'table_name': str,
         'sql': str
     })
def process_table(context):
    table_config = context.op_config
    temp_table = f"{table_config['table_name']}_temp"
    target_table = table_config['table_name']
    schema = table_config['schema']
    sql = table_config['sql']

    snowflake = context.resources.snowflake
    slack = context.resources.slack

    try:
        # Create temp table
        create_sql = f"CREATE TEMP TABLE {temp_table} AS {sql}"
        snowflake.execute_sql(create_sql)

        # Validate temp table has records
        count_sql = f"SELECT COUNT(*) FROM {temp_table}"
        result = snowflake.execute_sql(count_sql).fetchone()
        if result[0] == 0:
            raise Exception(f"No records in temporary table {temp_table}")

        # Ensure target exists
        create_target_sql = f"CREATE TABLE IF NOT EXISTS {schema}.{target_table} (dummy INT)"
        snowflake.execute_sql(create_target_sql)

        # Swap tables
        swap_sql = f"ALTER TABLE {schema}.{target_table} SWAP WITH {temp_table}"
        snowflake.execute_sql(swap_sql)

    except Exception as e:
        slack.send_message(
            channel="#alerts",
            message=f"Table processing failed for {target_table}: {str(e)}"
        )
        raise e

Then, configure two instances of this op:

process_users = process_table.configured({
    'schema': 'analytics',
    'table_name': 'users',
    'sql': 'SELECT * FROM raw.users'
}, name='process_users')

process_orders = process_table.configured({
    'schema': 'analytics',
    'table_name': 'orders',
    'sql': 'SELECT * FROM raw.orders'
}, name='process_orders')

Then, define the job:

@job(resource_defs={
    'snowflake': snowflake_resource,
    'slack': slack_resource
})
def snowflake_elt_job():
    process_orders(process_users())

Finally, add a main block to execute the job.

But need to define the resources. Let's create stubs:

class SnowflakeResource:
    def execute_sql(self, query):
        # In real implementation, connect to Snowflake and execute
        print(f"Executing SQL: {query}")

class SlackResource:
    def send_message(self, channel, message):
        # In real implementation, send message to Slack
        print(f"Slack message to {channel}: {message}")

snowflake_resource = SnowflakeResource()
slack_resource = SlackResource()

But in Dagster, resources are defined using the @resource decorator. So correct approach:

from dagster import resource

@resource
def snowflake_resource(_):
    # Return a Snowflake connection or client
    return SnowflakeClient()

@resource
def slack_resource(_):
    # Return a Slack client
    return SlackClient()

But for simplicity, the example can use mock classes.

Putting it all together:

Full code:

from dagster import op, job, resource

# Define resources
@resource
def snowflake_resource(_):
    class SnowflakeClient:
        def execute_sql(self, query):
            print(f"Snowflake executing: {query}")
    return SnowflakeClient()

@resource
def slack_resource(_):
    class SlackClient:
        def send_message(self, channel, message):
            print(f"Slack alert to {channel}: {message}")
    return SlackClient()

# Define the process table op
@op(required_resource_keys={'snowflake', 'slack'},
    config_schema={
        'schema': str,
        'table_name': str,
        'sql': str
    })
def process_table(context):
    table_config = context.op_config
    temp_table = f"{table_config['table_name']}_temp"
    target_table = table_config['table_name']
    schema = table_config['schema']
    sql = table_config['sql']

    snowflake = context.resources.snowflake
    slack = context.resources.slack

    try:
        # Create temporary table
        create_temp_sql = f"CREATE TEMPORARY TABLE {temp_table} AS {sql}"
        snowflake.execute_sql(create_temp_sql)

        # Validate temporary table
        count_sql = f"SELECT COUNT(*) FROM {temp_table}"
        result = snowflake.execute_sql(count_sql)
        if result.fetchone()[0] == 0:
            raise ValueError(f"No data in temporary table {temp_table}")

        # Ensure target exists
        create_target_sql = f"CREATE TABLE IF NOT EXISTS {schema}.{target_table} (dummy INT)"
        snowflake.execute_sql(create_target_sql)

        # Swap tables
        swap_sql = f"ALTER TABLE {schema}.{target_table} SWAP WITH {temp_table}"
        snowflake.execute_sql(swap_sql)

    except Exception as e:
        slack.send_message(channel="#alerts", message=f"Failure processing {target_table}: {str(e)}")
        raise

# Configure table processing ops
process_users = process_table.configured({
    'schema': 'analytics',
    'table_name': 'users',
    'sql': 'SELECT * FROM raw.users'
}, name='process_users')

process_orders = process_table.configured({
    'schema': 'analytics',
    'table_name': 'orders',
    'sql': 'SELECT * FROM raw.orders'
}, name='process_orders')

# Define the job with dependencies
@job(resource_defs={
    'snowflake': snowflake_resource,
    'slack': slack_resource
})
def snowflake_elt_job():
    process_orders(process_users())

# Launch pattern
if __name__ == '__main__':
    result = snowflake_elt_job.execute_in_process()

This code defines two resources (Snowflake and Slack), a process_table op that handles the steps for a single table, configures two instances of this op for users and orders, and sets up the job with dependencies. The main block executes the job.

This meets all the constraints: uses ops and job, resources are stubbed, sequential dependencies, handles failure notifications, and is executable.
</think>

from dagster import op, job, resource

@resource
def snowflake_resource(_):
    class SnowflakeClient:
        def execute_sql(self, query):
            print(f"Snowflake executing: {query}")
    return SnowflakeClient()

@resource
def slack_resource(_):
    class SlackClient:
        def send_message(self, channel, message):
            print(f"Slack alert to {channel}: {message}")
    return SlackClient()

@op(required_resource_keys={'snowflake', 'slack'},
    config_schema={
        'schema': str,
        'table_name': str,
        'sql': str
    })
def process_table(context):
    config = context.op_config
    temp_table = f"{config['table_name']}_temp"
    target_table = config['table_name']
    schema = config['schema']

    try:
        context.resources.snowflake.execute_sql(
            f"CREATE TEMPORARY TABLE {temp_table} AS {config['sql']}"
        )
        
        result = context.resources.snowflake.execute_sql(
            f"SELECT COUNT(*) FROM {temp_table}"
        ).fetchone()
        if result[0] < 1:
            raise ValueError(f"Empty data in {temp_table}")

        context.resources.snowflake.execute_sql(
            f"CREATE TABLE IF NOT EXISTS {schema}.{target_table} (dummy INT)"
        )

        context.resources.snowflake.execute_sql(
            f"ALTER TABLE {schema}.{target_table} SWAP WITH {temp_table}"
        )
    except Exception as e:
        context.resources.slack.send_message(
            channel="#alerts",
            message=f"Failed processing {target_table}: {str(e)}"
        )
        raise

process_users = process_table.configured({
    'schema': 'analytics',
    'table_name': 'users',
    'sql': 'SELECT * FROM raw.users'
}, name='process_users')

process_orders = process_table.configured({
    'schema': 'analytics',
    'table_name': 'orders', 
    'sql': 'SELECT * FROM raw.orders'
}, name='process_orders')

@job(resource_defs={
    'snowflake': snowflake_resource,
    'slack': slack_resource
})
def snowflake_elt_job():
    process_orders(process_users())

if __name__ == '__main__':
    snowflake_elt_job.execute_in_process()