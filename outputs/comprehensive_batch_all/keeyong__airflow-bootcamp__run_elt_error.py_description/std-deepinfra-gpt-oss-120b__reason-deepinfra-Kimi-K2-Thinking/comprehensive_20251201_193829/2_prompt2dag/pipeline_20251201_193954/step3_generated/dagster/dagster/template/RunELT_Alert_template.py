# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: RunELT_Alert
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T19:42:16.105119
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
    name="run_ctas",
    description="Run CTAS to Build Analytics Tables",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def run_ctas(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Run CTAS to Build Analytics Tables
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: run_ctas")
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
        
        context.log.info(f"Op run_ctas completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'run_ctas',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op run_ctas failed with return code {e.returncode}")
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
        context.log.error(f"Op run_ctas timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="notify_failure_slack",
    description="Slack Failure Notification",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def notify_failure_slack(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Slack Failure Notification
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: notify_failure_slack")
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
        
        context.log.info(f"Op notify_failure_slack completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'notify_failure_slack',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op notify_failure_slack failed with return code {e.returncode}")
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
        context.log.error(f"Op notify_failure_slack timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="runelt_alert",
    description="Sequential ELT pipeline that builds analytics tables in Snowflake using CTAS operations, with data validation, atomic swaps, and Slack failure notifications.",
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
    
    # Op: notify_failure_slack
    # Entry point - no dependencies
    notify_failure_slack_result = notify_failure_slack()
    
    # Op: run_ctas
    # Entry point - no dependencies
    run_ctas_result = run_ctas()


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