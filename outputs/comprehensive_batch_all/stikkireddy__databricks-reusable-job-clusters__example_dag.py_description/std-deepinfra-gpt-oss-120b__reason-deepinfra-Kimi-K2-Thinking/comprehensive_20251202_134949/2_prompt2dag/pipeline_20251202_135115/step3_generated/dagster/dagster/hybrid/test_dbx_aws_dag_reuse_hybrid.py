from dagster import op, job, in_process_executor, fs_io_manager, resource


@op(
    name='start_pipeline',
    description='Start Pipeline',
)
def start_pipeline(context):
    """Op: Start Pipeline"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='execute_primary_notebook',
    description='Execute Primary Databricks Notebook',
)
def execute_primary_notebook(context):
    """Op: Execute Primary Databricks Notebook"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='intermediate_dummy_1',
    description='Intermediate Dummy 1',
)
def intermediate_dummy_1(context):
    """Op: Intermediate Dummy 1"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='determine_branch_path',
    description='Determine Branch Path',
)
def determine_branch_path(context):
    """Op: Determine Branch Path"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='execute_secondary_notebook',
    description='Execute Secondary Databricks Notebook',
)
def execute_secondary_notebook(context):
    """Op: Execute Secondary Databricks Notebook"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='terminal_branch_dummy',
    description='Terminal Branch Dummy',
)
def terminal_branch_dummy(context):
    """Op: Terminal Branch Dummy"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='intermediate_dummy_2',
    description='Intermediate Dummy 2',
)
def intermediate_dummy_2(context):
    """Op: Intermediate Dummy 2"""
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


@resource
def databricks_default_resource(_):
    """Placeholder resource for Databricks."""
    return None


@job(
    name="test_dbx_aws_dag_reuse",
    description="Comprehensive pipeline that orchestrates Databricks notebook executions with conditional branching and reusable clusters. Manual trigger only.",
    executor_def=in_process_executor,
    resource_defs={
        "databricks_default": databricks_default_resource,
        "io_manager": fs_io_manager,
    },
)
def test_dbx_aws_dag_reuse():
    # Entry point
    start = start_pipeline()
    # Primary notebook execution
    primary = execute_primary_notebook(start)
    # Intermediate step
    dummy1 = intermediate_dummy_1(primary)
    # Branch decision
    branch = determine_branch_path(dummy1)
    # Fan‑out: two parallel branches
    terminal_branch_dummy(branch)          # Leaf branch
    secondary = execute_secondary_notebook(branch)  # Second branch
    # Continue after secondary branch
    dummy2 = intermediate_dummy_2(secondary)
    pipeline_completion(dummy2)