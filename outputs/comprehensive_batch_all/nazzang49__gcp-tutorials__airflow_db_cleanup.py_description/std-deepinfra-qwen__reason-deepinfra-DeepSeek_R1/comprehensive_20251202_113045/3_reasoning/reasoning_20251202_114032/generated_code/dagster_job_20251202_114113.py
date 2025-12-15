<think>
Okay, I need to convert the given Airflow pipeline into a Dagster job. Let's start by understanding the original pipeline's structure and requirements. The pipeline has two main tasks: print_configuration and cleanup_airflow_db. They run sequentially. 

First, in Dagster, tasks are represented as ops, and the job defines their dependencies. So I'll create two ops for each task. The print_configuration op will handle loading configuration, calculating the max date, and passing it to the next op. Since Dagster uses context for resources and config, I'll need to set that up.

The original uses XCom to pass data between tasks. In Dagster, ops can return values that are passed to downstream ops. So the print_configuration op will return the max_date, which cleanup_airflow_db will take as an input.

For configuration, I'll use Dagster's config system. The user might want to specify retention_days and enable_delete. So I'll define a config schema for both ops, or maybe have a shared configuration. Wait, the first task loads config from variables or defaults. So maybe the job's config will include these parameters. Let me structure the config as a dictionary with retention_days and enable_delete.

The print_configuration op will take the config, calculate max_date by subtracting retention_days from the current date. Then return that max_date. The cleanup_airflow_db op will receive max_date and the enable_delete flag to decide whether to perform deletions.

Next, the cleanup_airflow_db op needs to interact with the Airflow database. Since the original uses SQLAlchemy, I'll need a resource for the database session. But the user mentioned to provide simplified resources as comments or stubs. So I'll add a comment indicating a resource for the database session, maybe using a hypothetical AirflowDBResource class.

The op will iterate over predefined models/tables, build SQL queries, and conditionally delete based on enable_delete. Since the original has special cases for some models, I'll need to handle that. But for simplicity, maybe just loop through a list of models and apply the deletion logic. I'll stub out the actual database operations with comments.

Now, structuring the job. The two ops are linear, so the cleanup_airflow_db depends on print_configuration. In Dagster, the job's graph will define the execution order.

Testing if the code is executable: Need to make sure all imports are present. The ops use the Dagster context, config, and resources. The main block at the end allows execution in process.

Also, the original mentions email alerts on failure, but Dagster handles this via hooks or resources. Since the user wants minimal setup, maybe omit that part unless it's a comment.

Putting it all together:

- Imports: from dagster import op, job, resource, Config
- Define a config class (or use a dictionary) for retention_days and enable_delete.
- Create the print_configuration op that processes the config and returns max_date.
- Create the cleanup_airflow_db op that takes max_date and enable_delete, uses a database resource.
- Define the job with the two ops in sequence.
- Include a sample resource for the database (as a comment or stub).
- Add the main block to execute the job.

Wait, the user said if resources are needed, provide simplified examples as comments. So maybe the database resource is a commented-out @resource. Or use a mock resource.

Also, the original uses Airflow Variables, but in Dagster, config is handled via the job's configuration. So the print_configuration op will get its parameters from the job's config.

Let me draft the code:

First, the config. Using a Config class (from dagster 1.3+) or a plain dictionary. Since the user wants PEP8 and executable code, perhaps use a structured config.

But maybe for simplicity, use the context's op_config. So in the print_configuration op, access context.op_config.

Wait, the user's constraints mention if resources/config are needed, provide simplified examples. So perhaps the job has a config schema that includes retention_days and enable_delete.

The print_configuration op would take the retention_days from the config, compute max_date, and pass it. The cleanup_airflow_db op would take enable_delete from the config as well.

Wait, but how to pass enable_delete to the second op? Since in Dagster, each op can have its own config. Alternatively, the job's config could have both parameters, and both ops access their respective parts.

Alternatively, the job's config is passed to both ops. Let me structure the config as:

{
  'print_configuration': {'retention_days': 30},
  'cleanup_airflow_db': {'enable_delete': False}
}

But maybe it's better to have top-level config parameters since the original uses variables that are shared. So the job's config could be:

{
  'retention_days': 30,
  'enable_delete': False
}

Then both ops can access these parameters via their context. But how? Because each op's config is a subset. Alternatively, the first op uses retention_days, the second uses enable_delete. So in the job's config, those are top-level keys.

So in the print_configuration op, context.op_config['retention_days'].

In the cleanup_airflow_db op, context.op_config['enable_delete'].

But in Dagster, each op can have its own config section. So maybe the job's config is structured with op-specific configs. However, the original pipeline's parameters are shared between tasks. Hmm.

Alternatively, the job's config has 'retention_days' and 'enable_delete' at the top level, and both ops can access them via context.op_config, but that's only possible if the ops are configured to read those keys. Alternatively, use a shared configuration.

This might be a bit tricky. Let me think: The print_configuration task needs retention_days, and the cleanup task needs enable_delete. So the job's config should include both parameters. Each op can access the entire config, but perhaps each op only uses the relevant part.

Wait, in Dagster, when you define the op's config schema, you specify what keys it expects. But if we're not using strict config schemas (for simplicity), perhaps the ops just read from the context's op_config.

So, for the print_configuration op, it expects a 'retention_days' in the config. For cleanup_airflow_db, it expects 'enable_delete'.

But in the job, the config would need to have both keys. So when defining the job, the config would look like:

{
  'ops': {
    'print_configuration': {'config': {'retention_days': 30}},
    'cleanup_airflow_db': {'config': {'enable_delete': False}}
  }
}

But maybe the user wants to have retention_days and enable_delete as top-level config parameters. Alternatively, perhaps the job's config is a single set of parameters that both ops can access.

Alternatively, use a global config and pass the necessary parameters to each op via their inputs. Wait, the print_configuration op calculates max_date and passes it to cleanup_airflow_db. The enable_delete flag is part of the job's configuration and needs to be accessed by the cleanup op.

So perhaps the enable_delete is part of the cleanup op's config, while retention_days is part of the print_configuration's config.

But in the original pipeline, both parameters are part of the DAG's configuration (from variables). So in Dagster, the job's config would include both parameters, and each op takes what it needs.

This is getting a bit complicated. Let's proceed step by step.

First, the print_configuration op:

- It needs to get retention_days from the config. If not provided, default to, say, 30.

So in the op's config, there's a retention_days parameter.

Then, it calculates max_date as datetime.now() - retention_days days.

Then, returns max_date.

The cleanup_airflow_db op:

- Needs max_date from the first op.

- Also needs enable_delete from the config.

So the op's config has enable_delete, defaulting to False.

So the job's configuration would have two parts: one for each op.

But how to structure this in Dagster? Each op can have its own config schema.

So, the code would look like:

@op(config_schema={"retention_days": Field(int, default_value=30)})
def print_configuration(context):
    retention_days = context.op_config["retention_days"]
    # calculate max_date
    return max_date

@op(config_schema={"enable_delete": Field(bool, default_value=False)})
def cleanup_airflow_db(context, max_date):
    enable_delete = context.op_config["enable_delete"]
    # perform cleanup

Then, the job's config would need to specify the config for each op:

{
  "ops": {
    "print_configuration": {
      "config": {
        "retention_days": 30
      }
    },
    "cleanup_airflow_db": {
      "config": {
        "enable_delete": False
      }
    }
  }
}

But maybe the user wants to have retention_days and enable_delete as top-level parameters. Alternatively, perhaps the job's config is structured with those two parameters, and each op accesses the relevant one via context.job_config. But Dagster doesn't have a job-level config in that way; each op's config is nested under 'ops'.

Alternatively, use a shared configuration where both ops can access the same top-level keys. But that's not standard in Dagster. Each op's config is separate.

Hmm. To keep it simple, perhaps the print_configuration op's config includes retention_days, and the cleanup_airflow_db's config includes enable_delete. Then, when running the job, the user provides the config accordingly.

But the original pipeline's parameters are loaded from Airflow variables, which are global. So in Dagster, perhaps the job's config includes both parameters, and each op accesses their respective part.

So, the code would be:

@op
def print_configuration(context):
    retention_days = context.op_config.get("retention_days", 30)
    # calculate max_date
    return max_date

@op
def cleanup_airflow_db(context, max_date):
    enable_delete = context.op_config.get("enable_delete", False)
    # perform cleanup

But then, the job's config would need to pass these parameters to each op. So the config would be structured as:

{
  "ops": {
    "print_configuration": {
      "config": {
        "retention_days": 30
      }
    },
    "cleanup_airflow_db": {
      "config": {
        "enable_delete": False
      }
    }
  }
}

This way, each op gets its own config section. However, this might be a bit more complex for the user, but it's the correct way in Dagster.

Alternatively, if both parameters are part of the same config section, but that's not typical. Each op's config is separate.

But given the original pipeline uses variables that are shared between tasks, maybe the job's config has both parameters at the top level, and each op accesses them via context.job_config. But Dagster doesn't have a job-level config in that way. Each op's config is under 'ops' in the run config.

Alternatively, perhaps the job's config is structured with retention_days and enable_delete at the top level, and each op accesses them via context.op_config. But that would require that each op's config schema includes those keys, which may not be ideal.

This is getting a bit complicated. Let me proceed with the first approach: each op has its own config section. So the print_configuration op's config includes retention_days, and the cleanup_airflow_db's config includes enable_delete.

Now, coding this:

The print_configuration op will take retention_days from its config, compute max_date, and return it.

The cleanup_airflow_db op will take enable_delete from its config and max_date from the previous op.

Now, the database connection: The original uses SQLAlchemy. In Dagster, resources are used for external connections. So I'll define a resource, maybe called airflow_db, that provides a session. But the user said to provide simplified resources as comments or stubs. So perhaps add a resource definition as a comment.

For example:

# Example resource definition (would need proper SQLAlchemy setup):
# @resource(config_schema={"database_url": str})
# def airflow_db_resource(context):
#     from sqlalchemy import create_engine
#     engine = create_engine(context.resource_config["database_url"])
#     return engine.connect()

But in the code, since the user wants executable code, maybe mock the resource or use a dummy. However, the code needs to be executable. So perhaps the resource is not actually used, but stubbed.

Alternatively, within the cleanup_airflow_db op, we can have a comment indicating where the database interaction would happen.

Putting it all together:

The code would start with imports:

from dagster import op, job, resource, Field
from datetime import datetime, timedelta
from typing import List, Optional

Then, define the print_configuration op:

@op(config_schema={"retention_days": Field(int, default_value=30)})
def print_configuration(context):
    """Loads cleanup parameters and calculates max_date threshold."""
    retention_days = context.op_config["retention_days"]
    max_date = datetime.now() - timedelta(days=retention_days)
    context.log.info(f"Using retention period: {retention_days} days")
    context.log.info(f"Max date threshold: {max_date.isoformat()}")
    return max_date

Then, the cleanup_airflow_db op. It needs to take max_date and the enable_delete config:

@op(config_schema={"enable_delete": Field(bool, default_value=False)})
def cleanup_airflow_db(context, max_date):
    """Performs database cleanup based on max_date and configuration."""
    enable_delete = context.op_config["enable_delete"]
    context.log.info(f"Database cleanup started with enable_delete={enable_delete}")

    # Example model configuration (simplified)
    models_to_clean = [
        "DagRun",
        "TaskInstance",
        "Log",
        "XCom",
        "SlaMiss",
        "DagModel",
        # Other models as needed
    ]

    # Example database interaction (pseudo-code)
    for model in models_to_clean:
        context.log.info(f"Processing {model} records older than {max_date}")
        # Actual implementation would use SQLAlchemy session
        if enable_delete:
            context.log.warning(f"DELETING {model} records (simulated)")
        else:
            context.log.info(f"Dry run: Would delete {model} records")

    return len(models_to_clean)

Now, define the job:

@job
def airflow_db_cleanup_job():
    max_date = print_configuration()
    cleanup_airflow_db(max_date)

Finally, the main block:

if __name__ == "__main__":
    # Example configuration
    run_config = {
        "ops": {
            "print_configuration": {
                "config": {
                    "retention_days": 30
                }
            },
            "cleanup_airflow_db": {
                "config": {
                    "enable_delete": False
                }
            }
        }
    }
    result = airflow_db_cleanup_job.execute_in_process(run_config=run_config)

This code should be executable. The cleanup_airflow_db op simulates the database operations with logging. The resource for the database is not included here but could be added as a commented example.

The original pipeline mentions handling special cases for certain models. In the code, this is stubbed as a comment. The actual implementation would require more detailed logic, but for the purpose of this task, it's simplified.

The email alerts and retries are not implemented here, as per the user's constraints to keep it minimal. The scheduling and other orchestration details (like daily schedule) are part of the job's definition in Dagster, which would typically be handled by a schedule, but the user didn't ask for that in the code, just the job structure.

So the final code meets the requirements: uses ops and a job, handles config, passes data between ops, and includes a minimal execution example.
</think>

from datetime import datetime, timedelta
from dagster import op, job, Field

@op(
    config_schema={
        "retention_days": Field(int, default_value=30, description="Days to retain data"),
    }
)
def print_configuration(context):
    """Initialize cleanup parameters and calculate date threshold"""
    retention_days = context.op_config["retention_days"]
    max_date = datetime.now() - timedelta(days=retention_days)
    context.log.info(f"Calculated max date threshold: {max_date}")
    return max_date

@op(
    config_schema={
        "enable_delete": Field(bool, default_value=False, description="Toggle actual deletion"),
    }
)
def cleanup_airflow_db(context, max_date):
    """Execute database cleanup operations"""
    enable_delete = context.op_config["enable_delete"]
    context.log.info(f"Starting cleanup with delete enabled: {enable_delete}")
    
    # Example model configuration - would be expanded with actual ORM classes
    models = [
        "DagRun", "TaskInstance", "Log", "XCom", "SlaMiss",
        "DagModel", "TaskReschedule", "TaskFail", 
        "RenderedTaskInstanceFields", "ImportError", "Job"
    ]

    # Simplified cleanup simulation
    for model in models:
        context.log.info(f"Processing {model} records older than {max_date}")
        if enable_delete:
            context.log.warning(f"DELETING records from {model} (simulated)")
        else:
            context.log.info(f"Dry run: Would delete from {model}")

    return f"Processed {len(models)} tables"

@job
def airflow_database_maintenance():
    max_date = print_configuration()
    cleanup_airflow_db(max_date)

if __name__ == "__main__":
    # Example execution with test configuration
    run_config = {
        "ops": {
            "print_configuration": {"config": {"retention_days": 30}},
            "cleanup_airflow_db": {"config": {"enable_delete": False}}
        }
    }
    result = airflow_database_maintenance.execute_in_process(run_config=run_config)