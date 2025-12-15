from dagster import (
    op,
    job,
    in_process_executor,
    fs_io_manager,
    ResourceDefinition,
)

# Pre-generated task definitions (use exactly as provided)

@op(
    name='extract_user_content',
    description='Extract User Content',
)
def extract_user_content(context):
    """Op: Extract User Content"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='evaluate_toxicity',
    description='Evaluate Toxicity',
)
def evaluate_toxicity(context):
    """Op: Evaluate Toxicity"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='publish_content',
    description='Publish Safe Content',
)
def publish_content(context):
    """Op: Publish Safe Content"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='remove_and_flag_content',
    description='Remove and Flag Toxic Content',
)
def remove_and_flag_content(context):
    """Op: Remove and Flag Toxic Content"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='audit_log',
    description='Audit Log Consolidation',
)
def audit_log(context):
    """Op: Audit Log Consolidation"""
    # Docker execution
    # Image: python:3.9
    pass


# Dummy resources for required external systems
_dummy_resources = {
    "audit_logging_system": ResourceDefinition.hardcoded_resource(None),
    "platform_cms": ResourceDefinition.hardcoded_resource(None),
    "fs_local": ResourceDefinition.hardcoded_resource(None),
    "platform_publishing": ResourceDefinition.hardcoded_resource(None),
    "io_manager": fs_io_manager,
}


@job(
    name="content_moderation_pipeline",
    description="Comprehensive pipeline that scans user‑generated content for toxicity, branches based on a 0.7 threshold, and merges results for audit logging.",
    executor_def=in_process_executor,
    resource_defs=_dummy_resources,
)
def content_moderation_pipeline():
    # Entry point
    extract = extract_user_content()
    # Fan‑out after toxicity evaluation
    toxicity = evaluate_toxicity()
    extract >> toxicity

    # Branches
    remove = remove_and_flag_content()
    publish = publish_content()
    toxicity >> remove
    toxicity >> publish

    # Fan‑in to audit log
    audit = audit_log()
    remove >> audit
    publish >> audit