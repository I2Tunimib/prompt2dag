<think>
Okay, I need to convert the given healthcare claims processing ETL pipeline description into a Dagster job. Let me start by understanding the requirements.

First, the pipeline has an Extract stage with two parallel tasks: extract_claims and extract_providers. These should be grouped using a TaskGroup. Then there's the transform_join step that depends on both extract tasks. After that, the Load stage has two parallel tasks: load_warehouse and refresh_bi, which should also be in a TaskGroup.

Dagster uses ops and jobs, so each step will be an @op. The TaskGroups can be represented using subgraphs or by grouping ops, but since the user mentioned using ops and a job with dependencies, maybe using a graph with dependencies set appropriately. Wait, in Dagster, you can use a graph to define dependencies, and then create a job from that graph. Alternatively, maybe just structure the dependencies directly in the job.

Wait, the user specified to use @op and @job. So each task becomes an op, and the job sets the dependencies. The extract and load stages are task groups with parallel execution, which in Dagster terms means their ops don't depend on each other and can run in parallel.

So the dependencies are: extract_claims and extract_providers run in parallel. transform_join depends on both. Then load_warehouse and refresh_bi depend on transform_join and run in parallel.

The user also mentioned retry policy: 2 retries with 5-minute delays. In Dagster, you can set retry policies at the job level or individual ops. Since the example uses a job, maybe apply the retry policy to the job.

For resources, the pipeline uses CSV files and Postgres. So I'll need to create resources for these. The user said to provide simplified resource or config examples as comments or stubs. So maybe define a database_resource and a filesystem_resource as stubs.

The BI systems like Power BI and Tableau would need some way to trigger a refresh. Maybe the refresh_bi op calls an API or a command, but for simplicity, just log a message or use a stub.

Now, let's outline the code structure.

First, import necessary modules: dagster's op, job, resource, etc.

Define the resources. For example:

@resource(config_schema={"csv_path": str})
def csv_resource(context):
    return context.resource_config["csv_path"]

Similarly, a postgres_resource could be a stub.

Then, the ops:

- extract_claims and extract_providers would read CSV files. They might need the csv_resource to get the path. But since the user said to use minimal stubs, maybe pass the paths via config.

Wait, the user's constraints mention if resources/config are needed, provide simplified examples. So perhaps each extract op takes a config parameter for the CSV path. Alternatively, use a resource to manage file paths.

But to keep it simple, maybe have the extract_claims op take a config value for the claims_csv_path, and similarly for providers. Then, in the config, specify those paths.

But the user might prefer using resources. Let me think. The data sources are CSV files, so maybe the extract ops use a filesystem resource that provides the base path. Or perhaps the CSV paths are passed via op config.

Alternatively, for simplicity, have the ops take the CSV path as a config parameter. So in the op, context.op_config is used to get the path.

So, for extract_claims:

@op
def extract_claims(context):
    csv_path = context.op_config["claims_csv_path"]
    # read CSV and return data

Similarly for extract_providers.

Then, the transform_join op takes the outputs of both extract ops, joins them, anonymizes PII, calculates risk scores.

The load_warehouse op would take the transformed data and load it into Postgres, using a postgres resource.

The refresh_bi op would trigger BI refreshes, perhaps using a bi_resource that has methods for Power BI and Tableau.

But again, the user wants minimal stubs. So maybe the postgres resource is a mock, and the refresh_bi op just logs a message.

Putting it all together:

Define the ops with their dependencies. The extract ops are in a group (parallel), then transform depends on both, then load and refresh depend on transform.

In Dagster, dependencies are set by specifying the op's inputs as the outputs of previous ops. So transform_join would have inputs from extract_claims and extract_providers.

Then, load_warehouse and refresh_bi take the output of transform_join as input.

Wait, but the load_warehouse might need the transformed data, while refresh_bi might not need the data, just needs to run after transform. So perhaps transform_join outputs some data that load_warehouse uses, and refresh_bi doesn't need it. So in the op definitions, refresh_bi could depend on transform_join but not use its output.

In Dagster, you can have an op depend on another without taking its output by using the start_after parameter. But the standard way is to pass the output as an input. Alternatively, if the refresh_bi doesn't need the data, maybe it just starts after transform_join completes.

But how to model that? Since in Dagster, dependencies are determined by the data flow. So if an op's input is the output of another op, that creates a dependency. If refresh_bi doesn't need the data, perhaps it's better to have it start after transform_join by having it take the output as an input but not use it. Alternatively, use a dummy output from transform_join.

Alternatively, maybe the transform_join op returns a tuple of (transformed_data, trigger), where trigger is just a signal. Then refresh_bi can take the trigger as input. But that complicates the code.

Alternatively, have the transform_join op emit a result, and both load_warehouse and refresh_bi depend on it. Even if refresh_bi doesn't use the data, it's okay. So in code:

@op
def transform_join(claims_data, provider_data):
    # process and return transformed data
    return transformed_data

@op
def load_warehouse(transformed_data):
    # load data into warehouse

@op
def refresh_bi(transformed_data):
    # trigger refresh, ignoring the data

But in this case, refresh_bi would receive the transformed_data but not use it. That's acceptable.

Alternatively, maybe the transform_join op has two outputs: one for the data and one for a trigger. But that's more complex. For simplicity, just pass the data even if unused.

Now, the job would look like:

@job
def healthcare_claims_etl():
    claims = extract_claims()
    providers = extract_providers()
    transformed = transform_join(claims, providers)
    load_warehouse(transformed)
    refresh_bi(transformed)

This sets the dependencies correctly: claims and providers run in parallel, transform depends on both, then load and refresh depend on transform and run in parallel.

For resources, maybe the extract ops need the CSV paths. Let's model that via config. So each extract op has a config parameter for the CSV path.

For example, extract_claims could have a config schema that expects "claims_csv_path". Similarly for providers.

So in the op definition:

@op(config_schema={"csv_path": str})
def extract_claims(context):
    path = context.op_config["csv_path"]
    # read CSV and return data

Same for extract_providers.

Then, when configuring the job, the user would provide the paths in the config.

The load_warehouse op would need a Postgres resource. Let's define a mock Postgres resource as a stub.

@resource
def postgres_resource(_):
    # In a real scenario, this would return a connection or client
    return {"client": "postgres_client"}

Similarly, a bi_resource for the refresh_bi op.

@resource
def bi_resource(_):
    # Stub for BI tools
    return {"powerbi": "powerbi_client", "tableau": "tableau_client"}

Then, the ops that need these resources would require them as parameters.

For example:

@op(required_resource_keys={"postgres"})
def load_warehouse(context, transformed_data):
    postgres_client = context.resources.postgres
    # load data into warehouse using the client

@op(required_resource_keys={"bi"})
def refresh_bi(context, transformed_data):
    bi_clients = context.resources.bi
    # trigger refresh using the clients

Putting it all together, the job would be defined with the necessary resources.

The retry policy: the user wants 2 retries with 5-minute delays. In Dagster, you can set a retry policy at the job level using the `@job(retry_policy=...)` parameter.

So, import RetryPolicy from dagster.

from dagster import RetryPolicy

Then, define the retry policy:

retry_policy = RetryPolicy(max_retries=2, delay=300)  # 5 minutes in seconds

Then, apply it to the job:

@job(
    resource_defs={
        "postgres": postgres_resource,
        "bi": bi_resource,
    },
    config={
        "ops": {
            "extract_claims": {"config": {"csv_path": "default_claims.csv"}},
            "extract_providers": {"config": {"csv_path": "default_providers.csv"}},
        }
    },
    retry_policy=retry_policy,
)
def healthcare_claims_etl():
    # ... define the graph

Wait, but the config for the extract ops can be provided via the job's config. The example config in the job's config would set default paths, but in practice, the user would provide their own config at runtime.

Including a minimal launch pattern at the end:

if __name__ == "__main__":
    healthcare_claims_etl.execute_in_process(
        run_config={
            "ops": {
                "extract_claims": {"config": {"csv_path": "data/claims.csv"}},
                "extract_providers": {"config": {"csv_path": "data/providers.csv"}},
            }
        }
    )

Putting all this together, the code would look like:

Imports first:

from dagster import op, job, resource, RetryPolicy

Then define resources, ops, and the job.

Testing for PEP8 compliance, making sure all function names are snake_case, proper indentation, etc.

Also, the user mentioned that the code must be executable. So the resources and ops should have minimal implementations, like reading CSV with pandas, but if pandas isn't assumed, maybe use the csv module. However, the user said to assume required packages are installed. So perhaps use pandas for CSV reading.

But to avoid requiring pandas, maybe mock the data extraction. For example, the extract ops return a dictionary or a list of sample data.

For example:

@op(config_schema={"csv_path": str})
def extract_claims(context):
    # In practice, use pd.read_csv(context.op_config["csv_path"])
    context.log.info(f"Extracting claims from {context.op_config['csv_path']}")
    return [{"claim_id": 1, "patient_id": "123", "procedure_code": "XYZ", "amount": 100}]

Similarly for extract_providers.

The transform_join op would process the data, hash patient_id, calculate risk scores.

Then load_warehouse would take the transformed data and log or mock inserting into Postgres.

The refresh_bi op would log triggering the BI refresh.

So the code would be self-contained and executable without external dependencies.

Finally, the job includes the retry policy, resources, and config defaults.

So putting all together, here's the code.
</think>

from dagster import op, job, resource, RetryPolicy

@resource
def postgres_resource(_):
    return {"client": "postgres_connection"}

@resource
def bi_resource(_):
    return {"powerbi": "powerbi_api", "tableau": "tableau_api"}

@op(config_schema={"csv_path": str})
def extract_claims(context):
    context.log.info(f"Extracting claims from {context.op_config['csv_path']}")
    return [{"claim_id": 1, "patient_id": "P123", "provider_id": "DR456", "amount": 150.0}]

@op(config_schema={"csv_path": str})
def extract_providers(context):
    context.log.info(f"Extracting providers from {context.op_config['csv_path']}")
    return [{"provider_id": "DR456", "name": "MedCenter", "specialty": "Cardiology"}]

@op
def transform_join(context, claims_data, provider_data):
    context.log.info("Joining and transforming data")
    # Anonymize PII and calculate risk scores
    transformed = []
    for claim in claims_data:
        hashed_id = hash(claim["patient_id"])
        risk_score = claim["amount"] * 0.1  # Simplified risk calculation
        provider = next(p for p in provider_data if p["provider_id"] == claim["provider_id"])
        transformed.append({
            "claim_id": claim["claim_id"],
            "patient_hash": hashed_id,
            "provider_id": provider["provider_id"],
            "risk_score": risk_score
        })
    return transformed

@op(required_resource_keys={"postgres"})
def load_warehouse(context, transformed_data):
    context.log.info(f"Loading {len(transformed_data)} records to warehouse")
    # Example: context.resources.postgres["client"].execute_query(...)

@op(required_resource_keys={"bi"})
def refresh_bi(context, _transformed_data):
    context.log.info("Refreshing BI dashboards")
    # Example: context.resources.bi["powerbi"].refresh()
    # Example: context.resources.bi["tableau"].refresh()

@job(
    resource_defs={
        "postgres": postgres_resource,
        "bi": bi_resource
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def healthcare_claims_etl():
    claims = extract_claims()
    providers = extract_providers()
    transformed = transform_join(claims, providers)
    load_warehouse(transformed)
    refresh_bi(transformed)

if __name__ == "__main__":
    healthcare_claims_etl.execute_in_process(run_config={
        "ops": {
            "extract_claims": {"config": {"csv_path": "data/claims.csv"}},
            "extract_providers": {"config": {"csv_path": "data/providers.csv"}}
        }
    })