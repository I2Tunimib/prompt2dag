# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: run_ctas_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T03:27:59.445137
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
    description="Create Analytics Table via CTAS",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def run_ctas(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Create Analytics Table via CTAS
    
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
    name="slack_failure_alert",
    description="Slack Failure Notification",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def slack_failure_alert(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Slack Failure Notification
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: slack_failure_alert")
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
        
        context.log.info(f"Op slack_failure_alert completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'slack_failure_alert',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op slack_failure_alert failed with return code {e.returncode}")
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
        context.log.error(f"Op slack_failure_alert timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="run_ctas_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def run_ctas_pipeline():
    """
    Job: run_ctas_pipeline
    
    Pattern: sequential
    Ops: 2
    """
    # Execute ops in order based on dependencies
    
    # Op: run_ctas
    # Entry point - no dependencies
    run_ctas_result = run_ctas()
    
    # Op: slack_failure_alert
    # Single upstream dependency
    slack_failure_alert_result = slack_failure_alert()


# --- Resources ---
from dagster import resource

@resource
def snowflake_conn_resource(context):
    """Resource: snowflake_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "snowflake_conn"}

@resource
def slack_conn_resource(context):
    """Resource: slack_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "slack_conn"}



# --- Repository ---
from dagster import repository

@repository
def run_ctas_pipeline_repository():
    """Repository containing run_ctas_pipeline job."""
    return [run_ctas_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = run_ctas_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")