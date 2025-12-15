from dagster import job, op, graph, In, Out, ResourceDefinition, fs_io_manager, docker_executor

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
    name='execute_primary_notebook',
    description='Execute Primary Notebook',
)
def execute_primary_notebook(context):
    """Op: Execute Primary Notebook"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='intermediate_step_1',
    description='Intermediate Step 1',
)
def intermediate_step_1(context):
    """Op: Intermediate Step 1"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='branch_decision',
    description='Branch Decision',
)
def branch_decision(context):
    """Op: Branch Decision"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='execute_secondary_notebook',
    description='Execute Secondary Notebook',
)
def execute_secondary_notebook(context):
    """Op: Execute Secondary Notebook"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='terminal_branch_task',
    description='Terminal Branch Task',
)
def terminal_branch_task(context):
    """Op: Terminal Branch Task"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='intermediate_step_2',
    description='Intermediate Step 2',
)
def intermediate_step_2(context):
    """Op: Intermediate Step 2"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='pipeline_completion',
    description='Pipeline Completion',
)
def pipeline_completion(context):
    """Op: Pipeline Completion"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="initialize_pipeline_pipeline",
    description="No description provided.",
    executor_def=docker_executor,
    resource_defs={"io_manager": fs_io_manager, "databricks_default": ResourceDefinition.hardcoded_resource(None)},
)
def initialize_pipeline_pipeline():
    init_pipeline = initialize_pipeline()
    execute_primary = execute_primary_notebook(init_pipeline)
    intermediate_1 = intermediate_step_1(execute_primary)
    branch_dec = branch_decision(intermediate_1)
    
    terminal_branch = terminal_branch_task(branch_dec)
    execute_secondary = execute_secondary_notebook(branch_dec)
    
    intermediate_2 = intermediate_step_2(execute_secondary)
    pipeline_completion(intermediate_2)