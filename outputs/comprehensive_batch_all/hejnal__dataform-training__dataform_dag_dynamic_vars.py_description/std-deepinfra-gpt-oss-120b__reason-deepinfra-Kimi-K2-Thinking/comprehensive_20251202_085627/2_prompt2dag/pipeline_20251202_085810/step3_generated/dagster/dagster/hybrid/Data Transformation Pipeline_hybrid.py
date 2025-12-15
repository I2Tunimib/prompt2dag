from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition

@op(
    name='initialize_pipeline',
    description='Initialize Pipeline',
)
def initialize_pipeline(context):
    """Op: Initialize Pipeline"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='parse_input_params',
    description='Parse Input Parameters',
)
def parse_input_params(context):
    """Op: Parse Input Parameters"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='create_compilation_result',
    description='Create Dataform Compilation Result',
)
def create_compilation_result(context):
    """Op: Create Dataform Compilation Result"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='create_workflow_invocation',
    description='Create Dataform Workflow Invocation',
)
def create_workflow_invocation(context):
    """Op: Create Dataform Workflow Invocation"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='monitor_workflow_state',
    description='Monitor Workflow Invocation State',
)
def monitor_workflow_state(context):
    """Op: Monitor Workflow Invocation State"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='finalize_pipeline',
    description='Finalize Pipeline',
)
def finalize_pipeline(context):
    """Op: Finalize Pipeline"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='data_transformation_pipeline',
    description='Comprehensive Pipeline Description',
    executor_def=in_process_executor,
    resource_defs={'modelling_cloud_default': ResourceDefinition.hardcoded_resource(None)},
    io_manager_defs={'fs_io_manager': fs_io_manager},
)
def data_transformation_pipeline():
    init = initialize_pipeline()
    parse = parse_input_params()
    compile = create_compilation_result()
    invoke = create_workflow_invocation()
    monitor = monitor_workflow_state()
    finalize = finalize_pipeline()

    init >> parse >> compile >> invoke >> monitor >> finalize