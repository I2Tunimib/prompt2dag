from dagster import (
    op,
    job,
    Out,
    In,
    Nothing,
    RetryPolicy,
    schedule,
    Definitions,
    resource,
    Field,
    HookContext,
    failure_hook,
)
from typing import Dict, List, Any
import pandas as pd
from datetime import datetime
import os


# Resources
@resource(config_schema={"base_path": Field(str, default_value="/data")})
def data_resource(context):
    """Resource for accessing data files."""
    return {"base_path": context.resource_config["base_path"]}


@resource(config_schema={"rates": Field(dict, default_value={"EUR": 1.1, "JPY": 0.007})})
def exchange_rate_resource(context):
    """Resource for currency exchange rates."""
    return context.resource_config["rates"]


@resource
def email_resource(_):
    """Stub email resource for notifications."""
    return {"smtp_host": "localhost", "from_email": "dagster@example.com"}


# Hooks for notifications
@failure_hook(required_resource_keys={"email"})
def email_notification(context: HookContext):
    """Send email notification on failure."""
    print(
        f"[EMAIL ALERT] Job '{context.job_name}' op '{context.op.name}' failed!"
    )


# Ops
@op(
    out=Out(Nothing),
    description="Marks the start of the pipeline",
)
def start_pipeline(context):
    """Initialize pipeline execution."""
    context.log.info("Starting multi-region ecommerce analytics pipeline")
    return None


@op(
    out=Out(Dict[str, Any]),
    required_resource_keys={"data"},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest US-East region sales data from CSV",
)
def ingest_us_east(context) ->