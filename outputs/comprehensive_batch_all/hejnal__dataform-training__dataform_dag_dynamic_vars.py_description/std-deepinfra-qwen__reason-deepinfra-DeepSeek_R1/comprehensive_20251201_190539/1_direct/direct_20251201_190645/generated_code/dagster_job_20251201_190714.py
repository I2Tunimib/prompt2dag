from dagster import op, job, In, Out, String, Field, resource, execute_in_process

# Simplified resource example for Google Cloud connection
@resource(config_schema={"gcp_conn_id": Field(String, default_value="modelling_cloud_default")})
def gcp_dataform_resource(context):
    return context.resource_config["gcp_conn_id"]

# Dummy operator to start the pipeline
@op
def start_pipeline():
    """Dummy operator to initiate the pipeline execution."""
    pass

# Parse input parameters and store in XCom
@op(required_resource_keys={"gcp_dataform"}, out={"compilation_config": Out(String)})
def parse_input_parameters(context):
    """Extract and process runtime parameters including logical date and description."""
    dag_run = context.op_config.get("dag_run", {})
    logical_date = dag_run.get("logical_date", "2023-01-01")
    description = dag_run.get("description", "Default description")
    compilation_config = f"Compilation config for {logical_date} - {description}"
    return compilation_config

# Create compilation result using parsed parameters
@op(required_resource_keys={"gcp_dataform"}, ins={"compilation_config": In(String)})
def create_compilation_result(context, compilation_config):
    """Retrieve the configuration from XCom and execute Dataform compilation."""
    gcp_conn_id = context.resources.gcp_dataform
    # Simulate Dataform compilation
    context.log.info(f"Creating compilation result with config: {compilation_config} using {gcp_conn_id}")
    compilation_result = "Compilation result ID"
    return compilation_result

# Trigger Dataform workflow execution asynchronously
@op(required_resource_keys={"gcp_dataform"}, ins={"compilation_result": In(String)})
def create_workflow_invocation(context, compilation_result):
    """Trigger the Dataform workflow execution asynchronously."""
    gcp_conn_id = context.resources.gcp_dataform
    # Simulate workflow invocation
    context.log.info(f"Creating workflow invocation with compilation result: {compilation_result} using {gcp_conn_id}")
    workflow_invocation_id = "Workflow invocation ID"
    return workflow_invocation_id

# Monitor the workflow invocation until it reaches SUCCEEDED or FAILED state
@op(required_resource_keys={"gcp_dataform"}, ins={"workflow_invocation_id": In(String)})
def is_workflow_invocation_done(context, workflow_invocation_id):
    """Monitor the workflow invocation until it reaches either SUCCEEDED or FAILED state."""
    gcp_conn_id = context.resources.gcp_dataform
    # Simulate workflow state monitoring
    context.log.info(f"Monitoring workflow invocation: {workflow_invocation_id} using {gcp_conn_id}")
    state = "SUCCEEDED"  # Simulate success
    return state

# Dummy operator to mark pipeline completion
@op
def end_pipeline():
    """Dummy operator to mark pipeline completion."""
    pass

# Define the job with the specified dependencies
@job(resource_defs={"gcp_dataform": gcp_dataform_resource})
def dataform_pipeline():
    start = start_pipeline()
    compilation_config = parse_input_parameters(start)
    compilation_result = create_compilation_result(compilation_config)
    workflow_invocation_id = create_workflow_invocation(compilation_result)
    workflow_state = is_workflow_invocation_done(workflow_invocation_id)
    end_pipeline(workflow_state)

# Minimal launch pattern
if __name__ == '__main__':
    result = dataform_pipeline.execute_in_process()