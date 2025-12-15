# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: RunELT_Alert
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T09:28:31.516664
# ==============================================================================

from __future__ import annotations

import os
import subprocess
from typing import Dict, Any

from dagster import (
    op,
    job,
    In,
    Out,
    Nothing,
    OpExecutionContext,
    Failure,
    RetryPolicy,
)
from dagster import in_process_executor

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/dagster/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Op Definitions ---

@op(
    name="create_analytics_table",
    description="Create Analytics Table via CTAS",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def create_analytics_table(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Create Analytics Table via CTAS
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: create_analytics_table")
    context.log.info(f"Image: ")
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    context.log.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=3600,
        )
        
        context.log.info(f"Op create_analytics_table completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'create_analytics_table',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op create_analytics_table failed with return code {e.returncode}")
        context.log.error(f"STDERR: {e.stderr}")
        raise Failure(
            description=f"Docker container failed: {e.stderr}",
            metadata={
                'return_code': e.returncode,
                'stdout': e.stdout,
                'stderr': e.stderr,
            }
        )
    except subprocess.TimeoutExpired as e:
        context.log.error(f"Op create_analytics_table timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="slack_failure_notifier",
    description="Slack Failure Notifier",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=30,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def slack_failure_notifier(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Slack Failure Notifier
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: slack_failure_notifier")
    context.log.info(f"Image: ")
    
    # Build docker run command
    cmd = [
        'docker', 'run',
        '--rm',
        '-v', f'{HOST_DATA_DIR}:{CONTAINER_DATA_DIR}',
    ]
    
    # Add environment variables
    
    # Add image
    cmd.append('')
    
    # Add command if specified
    
    # Execute
    context.log.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=3600,
        )
        
        context.log.info(f"Op slack_failure_notifier completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'slack_failure_notifier',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op slack_failure_notifier failed with return code {e.returncode}")
        context.log.error(f"STDERR: {e.stderr}")
        raise Failure(
            description=f"Docker container failed: {e.stderr}",
            metadata={
                'return_code': e.returncode,
                'stdout': e.stdout,
                'stderr': e.stderr,
            }
        )
    except subprocess.TimeoutExpired as e:
        context.log.error(f"Op slack_failure_notifier timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="runelt_alert",
    description="Comprehensive Pipeline Description",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def runelt_alert():
    """
    Job: RunELT_Alert
    
    Pattern: sequential
    Ops: 2
    """
    # Execute ops in order based on dependencies
    
    # Op: create_analytics_table
    # Entry point - no dependencies
    create_analytics_table_result = create_analytics_table()
    
    # Op: slack_failure_notifier
    # Single upstream dependency
    slack_failure_notifier_result = slack_failure_notifier()


# --- Resources ---
from dagster import resource

@resource
def slack_conn_resource(context):
    """Resource: slack_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "slack_conn"}

@resource
def snowflake_conn_resource(context):
    """Resource: snowflake_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "snowflake_conn"}



# --- Repository ---
from dagster import repository

@repository
def runelt_alert_repository():
    """Repository containing runelt_alert job."""
    return [runelt_alert]


if __name__ == "__main__":
    # Execute job locally for testing
    result = runelt_alert.execute_in_process()
    print(f"Job execution result: {result.success}")