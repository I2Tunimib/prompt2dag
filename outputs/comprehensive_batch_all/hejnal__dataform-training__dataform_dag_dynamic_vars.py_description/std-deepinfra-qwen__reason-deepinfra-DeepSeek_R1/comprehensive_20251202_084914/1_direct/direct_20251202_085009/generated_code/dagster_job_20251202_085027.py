from dagster import op, job, resource, RetryPolicy, In, Out, Nothing, Field, String, execute_in_process

# Simplified resource for Google Cloud Dataform connection
@resource(config_schema={"connection_id": Field(String, default_value="modelling_cloud_default")})
def dataform_resource(context):
    return {"connection_id": context.resource_config["connection_id"]}

# Dummy start and end operators
@op
def start(context):
    context.log.info("Pipeline started")

@op
def end(context):
    context.log.info("Pipeline completed")

# Parse input parameters and store in XCom
@op(required_resource_keys={"dataform"}, out={"compilation_config": Out()})
def parse_input_parameters(context):
    dag_run_conf = context.op_config.get("dag_run_conf", {})
    logical_date = dag_run_conf.get("logical_date", "2023-01-01")
    description = dag_run_conf.get("description", "Default description")
    compilation_config = {
        "logical_date": logical_date,
        "description": description,
        "git_commitish": dag_run_conf.get("git_commitish", "main")
    }
    context.log.info(f"Compilation configuration: {compilation_config}")
    return compilation_config

# Create Dataform compilation result
@op(required_resource_keys={"dataform"}, ins={"compilation_config": In()}, out={"compilation_result": Out()})
def create_compilation_result(context, compilation_config):
    context.log.info(f"Creating compilation result with config: {compilation_config}")
    compilation_result = {"id": "12345", "status": "COMPILED"}
    context.log.info(f"Compilation result: {compilation_result}")
    return compilation_result

# Create Dataform workflow invocation
@op(required_resource_keys={"dataform"}, ins={"compilation_result": In()})
def create_workflow_invocation(context, compilation_result):
    context.log.info(f"Creating workflow invocation with compilation result: {compilation_result}")
    workflow_invocation = {"id": "67890", "status": "RUNNING"}
    context.log.info(f"Workflow invocation: {workflow_invocation}")

# Monitor Dataform workflow invocation state
@op(required_resource_keys={"dataform"}, ins={"compilation_result": In()})
def is_workflow_invocation_done(context, compilation_result):
    context.log.info(f"Monitoring workflow invocation with compilation result: {compilation_result}")
    # Simulate monitoring loop
    while True:
        # Check status
        status = "SUCCEEDED"  # Simplified for example
        context.log.info(f"Workflow invocation status: {status}")
        if status in ["SUCCEEDED", "FAILED"]:
            break

# Define the job
@job(
    resource_defs={"dataform": dataform_resource},
    retry_policy=RetryPolicy(max_retries=0),
)
def dataform_pipeline():
    start_op = start()
    parse_op = parse_input_parameters(start_op)
    create_compilation_op = create_compilation_result(parse_op)
    create_workflow_op = create_workflow_invocation(create_compilation_op)
    monitor_op = is_workflow_invocation_done(create_workflow_op)
    end_op = end(monitor_op)

# Execute the job
if __name__ == '__main__':
    result = dataform_pipeline.execute_in_process()