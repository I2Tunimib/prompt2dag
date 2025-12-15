<think>
Okay, I need to convert the given Airflow pipeline into a Dagster job. Let's start by understanding the original pipeline's structure. The pipeline has two tasks: print_configuration and cleanup_airflow_db. They run sequentially. 

First, in Dagster, each task becomes an op. So I'll create two ops using @op decorators. The dependencies are linear, so the cleanup_airflow_db op depends on print_configuration.

The print_configuration task loads configuration, calculates the max_date, and pushes it to XCom. In Dagster, instead of XCom, we can pass outputs between ops directly. So the print_configuration op will return the max_date, which cleanup_airflow_db will take as an input.

For configuration, Dagster uses a config schema. The user might want to pass retention_days and enable_delete. I'll define a config structure with these fields. The print_configuration op will read the retention_days from the config, compute max_date, and pass it along.

The cleanup_airflow_db op needs to handle database operations. Since it's using SQLAlchemy, I'll need a resource for the database session. I'll create a simple resource stub for that. The op will take the max_date and the enable_delete flag from the config. It will iterate over the tables and perform deletions conditionally.

Wait, the original pipeline uses Airflow Variables and XCom. In Dagster, the config can be handled via the job's configuration, so I'll set default values there. The email alerts and retries mentioned in the original are part of Airflow's setup, but in Dagster, those would be handled differently, maybe with hooks or retry policies. However, the user constraints say to provide minimal stubs, so maybe I can skip those for now unless necessary.

The job should have a daily schedule, but the code example doesn't include scheduling. The user asked for the module code, so maybe the schedule isn't part of this. The main thing is to create the job with the two ops.

I need to structure the ops with their inputs and outputs. The print_configuration op will output the max_date, which the cleanup op uses. The cleanup op also needs the enable_delete flag from the config. Wait, how to pass both the config and the max_date? The config is available in the ops via context.op_config, but the max_date is passed as an input from the previous op.

So, the cleanup_airflow_db op will have two inputs: the max_date from the first op, and the enable_delete from the job's config. Wait, no. The config is per-op, or can be part of the job's overall config. Let me think. Each op can have its own config, but in this case, the enable_delete is part of the overall job's configuration, shared between both ops? Or maybe the cleanup op needs it as part of its config.

Alternatively, the job's config could have retention_days and enable_delete. The print_configuration op uses retention_days to compute max_date, and the cleanup op uses enable_delete. So the print_configuration op's config would include retention_days, and the cleanup op's config would include enable_delete. But in Dagster, each op can have its own config schema. However, the user might want to have a single config for the job. Let me check.

Looking at the original pipeline, the configuration is loaded once in print_configuration, which includes both retention_days and enable_delete (probably). So maybe the job's config has both parameters, and both ops access their respective parts. For example, the print_configuration op uses 'retention_days', and the cleanup op uses 'enable_delete'.

So, in the job's config, we have:

{
  'retention_days': int,
  'enable_delete': bool
}

Each op can access their part via context.op_config. So in the print_configuration op, context.op_config['retention_days'], and in the cleanup op, context.op_config['enable_delete'].

Wait, but in Dagster, when you define the config for a job, each op's config is nested under the op's name. But if the job is using a single config that's shared among ops, maybe using a shared resource or config. Alternatively, perhaps the config is structured with the op's config under their keys. But the original pipeline seems to have a single configuration that's used by both tasks. So perhaps the job's config is at the top level with 'retention_days' and 'enable_delete', and each op accesses these via their own config. But that might require that each op's config includes those keys. Alternatively, maybe the job's config is passed as a global config, and each op can access the necessary parts.

Hmm, this is a bit tricky. Let me think. In Dagster, each op can have its own config schema. So for the print_configuration op, its config would expect 'retention_days', and the cleanup_airflow_db op's config would expect 'enable_delete'. Then, when configuring the job, the user would provide these under the op's names. But the original pipeline's configuration is loaded once, so maybe the job's config is structured with those two parameters at the top level, and each op's config is mapped to those. Alternatively, perhaps the job's config is a single dictionary with those keys, and each op reads their respective key from the job's config.

Wait, perhaps the job's config is structured as:

{
  'ops': {
    'print_configuration': {
      'config': {
        'retention_days': 30
      }
    },
    'cleanup_airflow_db': {
      'config': {
        'enable_delete': False
      }
    }
  }
}

But that's more nested. Alternatively, maybe the job has a top-level config with 'retention_days' and 'enable_delete', and each op accesses those via the context.job_config. But Dagster doesn't work that way. Each op's config is scoped to the op.

Alternatively, perhaps the print_configuration op is responsible for reading both parameters and passing enable_delete along with max_date to the cleanup op. But that would require the print_configuration op to have both parameters in its config.

Wait, the original print_configuration task loads configuration from DAG run variables or falls back to defaults. So maybe in Dagster, the job's config includes both 'retention_days' and 'enable_delete', and the print_configuration op reads 'retention_days' to compute max_date, and passes that to the cleanup op. The cleanup op then needs 'enable_delete' from its own config.

But how to structure that. Let's see. The print_configuration op's config has 'retention_days', and the cleanup op's config has 'enable_delete'. Then, when defining the job, the user would need to provide both under their respective ops. But the original pipeline seems to have these as top-level parameters. So perhaps the job's config is structured with those two parameters, and each op's config is mapped to them. But I'm not sure.

Alternatively, maybe the job's config is a single config that includes both parameters, and each op accesses the parameters they need via the context. But in Dagster, each op's config is separate. So perhaps the job's config is defined with both parameters, and each op's config is a subset. For example:

@job(config={
  'ops': {
    'print_configuration': {
      'config': {
        'retention_days': 30
      }
    },
    'cleanup_airflow_db': {
      'config': {
        'enable_delete': False
      }
    }
  }
})

But that would require the user to configure each op separately. However, the original pipeline's configuration is loaded once, so perhaps the print_configuration op reads both retention_days and enable_delete, computes max_date, and passes both max_date and enable_delete to the cleanup op. But that would couple the two parameters together. Alternatively, maybe enable_delete is part of the cleanup op's config, and retention_days is part of the print_configuration's config.

In that case, the print_configuration op's config has 'retention_days', and the cleanup op's config has 'enable_delete'. Then, the job's config would need to specify both under their respective ops. That seems feasible.

So, the code would look like:

@op(config_schema={'retention_days': int})
def print_configuration(context):
    retention_days = context.op_config['retention_days']
    max_date = ... # calculate
    return max_date

@op(config_schema={'enable_delete': bool})
def cleanup_airflow_db(context, max_date):
    enable_delete = context.op_config['enable_delete']
    # perform cleanup

@job
def airflow_db_cleanup_job():
    max_date = print_configuration()
    cleanup_airflow_db(max_date)

But then, when running the job, the user would need to provide config for both ops. For example:

{
  'ops': {
    'print_configuration': {
      'config': {
        'retention_days': 30
      }
    },
    'cleanup_airflow_db': {
      'config': {
        'enable_delete': True
      }
    }
  }
}

That's acceptable. The original pipeline's parameters are split between the two ops. The print_configuration handles retention_days, and the cleanup handles enable_delete.

Now, for the database resource. The cleanup op needs a database session. So we can define a resource, maybe 'airflow_db', which provides a session. The resource can be a simple stub using SQLAlchemy.

So, the resource would be something like:

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class AirflowDatabaseResource:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.Session()

Then, in the job, we can define the resource:

from dagster import resource

@resource(config_schema={'connection_string': str})
def airflow_db_resource(context):
    return AirflowDatabaseResource(context.resource_config['connection_string'])

But the user constraints say to provide simplified resource examples as comments or minimal stubs. So maybe just include a comment indicating that a resource is needed, and in the code, pass it as a stub.

Alternatively, in the cleanup op, we can have the resource as a parameter. So the op would require the 'airflow_db' resource.

But how to structure that. Let's see. The cleanup_airflow_db op would need to use the resource to get a session. So in the op definition, we can include a resource requirement.

So, modifying the cleanup op:

@op(required_resource_keys={'airflow_db'}, config_schema={'enable_delete': bool})
def cleanup_airflow_db(context, max_date):
    session = context.resources.airflow_db.get_session()
    # perform cleanup with session

Then, the job would need to include the resource:

@job(resource_defs={'airflow_db': airflow_db_resource})
def airflow_db_cleanup_job():
    cleanup_airflow_db(print_configuration())

But the resource needs a connection string. So in the configuration, the resource would have its own config, like:

resources:
  airflow_db:
    config:
      connection_string: 'sqlite:///airflow.db'

But again, the user wants minimal stubs. So perhaps in the code, we can define the resource with a default connection string, or leave it as a comment.

Putting it all together:

The print_configuration op takes retention_days from its config, computes max_date, returns it. The cleanup_airflow_db op takes max_date as input, uses enable_delete from its own config, and uses the airflow_db resource to get a session.

Now, handling the database models. The original pipeline iterates through predefined database objects. So in the cleanup op, there's a list of models and their configurations. For simplicity, maybe we can hardcode a list of models and their table names, or pass it as a config. But the user said to avoid placeholders, so perhaps hardcode a list of models as a comment or a list variable.

The cleanup op would loop through each model, build SQL queries based on max_date, and conditionally delete if enable_delete is True.

So, in code:

def cleanup_airflow_db(context, max_date):
    enable_delete = context.op_config['enable_delete']
    session = context.resources.airflow_db.get_session()
    # Predefined models configuration
    models_config = [
        {'model': 'DagRun', 'date_field': 'execution_date'},
        # ... other models
    ]
    for config in models_config:
        # Build query
        # Execute
        # Delete if enabled
    # Handle special cases

But since we can't actually implement all the SQL logic here, we can stub it with comments.

Putting all this together, the final code would look like:

from dagster import op, job, resource
from datetime import datetime, timedelta

# Example resource definition (simplified)
@resource(config_schema={'connection_string': str})
def airflow_db_resource(context):
    # In reality, this would create a SQLAlchemy session
    return {'session': context.resource_config['connection_string']}

@op(config_schema={'retention_days': int})
def print_configuration(context):
    retention_days = context.op_config['retention_days']
    max_date = datetime.now() - timedelta(days=retention_days)
    context.log.info(f"Using retention days: {retention_days}, max_date: {max_date}")
    return max_date

@op(required_resource_keys={'airflow_db'}, config_schema={'enable_delete': bool})
def cleanup_airflow_db(context, max_date):
    enable_delete = context.op_config['enable_delete']
    session = context.resources.airflow_db['session']  # Simplified session access
    context.log.info(f"Cleaning up with max_date: {max_date}, deletions enabled: {enable_delete}")
    
    # Example model configuration
    models = [
        {'name': 'DagRun', 'date_field': 'execution_date'},
        {'name': 'TaskInstance', 'date_field': 'execution_date'},
        # Add other models as needed
    ]
    
    for model in models:
        context.log.info(f"Processing {model['name']} where {model['date_field']} < {max_date}")
        if enable_delete:
            context.log.info(f"Deleting old {model['name']} records")
            # Actual deletion logic would go here
    
    # Handle special cases
    context.log.info("Completed cleanup")

@job(resource_defs={'airflow_db': airflow_db_resource})
def airflow_db_cleanup_job():
    cleanup_airflow_db(print_configuration())

if __name__ == '__main__':
    # Example execution with sample configuration
    result = airflow_db_cleanup_job.execute_in_process(run_config={
        'ops': {
            'print_configuration': {'config': {'retention_days': 30}},
            'cleanup_airflow_db': {'config': {'enable_delete': False}}
        },
        'resources': {
            'airflow_db': {'config': {'connection_string': 'sqlite:///airflow.db'}}
        }
    })

Wait, but the resource 'airflow_db' is defined with a config_schema that expects 'connection_string'. In the execute_in_process, the run_config includes the resource's config under 'resources' key.

This code should be executable, with the stubbed resource and ops. The print_configuration op returns max_date, which is passed to cleanup_airflow_db. The cleanup op uses the resource to get a session (simplified here as just the connection string for logging), and processes each model.

I think this meets all the constraints: uses ops and job, minimal resources and config, explicit dependencies, and a main block for execution.
</think>

from datetime import datetime, timedelta
from dagster import op, job, resource

# Simplified database resource stub
@resource(config_schema={"connection_string": str})
def airflow_db_resource(context):
    class SessionStub:
        def execute(self, query):
            print(f"Executing: {query}")
            
    return {"session": SessionStub(), "connection": context.resource_config["connection_string"]}

@op(config_schema={"retention_days": int})
def print_configuration(context):
    retention_days = context.op_config["retention_days"]
    max_date = datetime.now() - timedelta(days=retention_days)
    context.log.info(f"Calculated max date threshold: {max_date}")
    return max_date.isoformat()

@op(
    required_resource_keys={"airflow_db"},
    config_schema={"enable_delete": bool}
)
def cleanup_airflow_db(context, max_date):
    models_config = [
        {"table": "dag_run", "date_column": "execution_date"},
        {"table": "task_instance", "date_column": "execution_date"},
        {"table": "log", "date_column": "dttm"},
        {"table": "xcom", "date_column": "timestamp"},
    ]
    
    for model in models_config:
        query = f"""
            DELETE FROM {model['table']}
            WHERE {model['date_column']} < '{max_date}'
        """ if context.op_config["enable_delete"] else f"""
            SELECT COUNT(*) FROM {model['table']}
            WHERE {model['date_column']} < '{max_date}'
        """
        
        result = context.resources.airflow_db["session"].execute(query)
        context.log.info(f"Processed {model['table']}. Result: {result}")

@job(resource_defs={"airflow_db": airflow_db_resource})
def airflow_database_maintenance_job():
    cleanup_airflow_db(print_configuration())

if __name__ == "__main__":
    result = airflow_database_maintenance_job.execute_in_process(
        run_config={
            "ops": {
                "print_configuration": {"config": {"retention_days": 365}},
                "cleanup_airflow_db": {"config": {"enable_delete": False}}
            },
            "resources": {
                "airflow_db": {"config": {"connection_string": "sqlite:///airflow.db"}}
            }
        }
    )