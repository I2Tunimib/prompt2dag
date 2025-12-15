# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: run_after_loop_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-03T00:02:23.191841
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
    name="run_after_loop",
    description="Run After Loop",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def run_after_loop(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Run After Loop
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: run_after_loop")
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
        
        context.log.info(f"Op run_after_loop completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'run_after_loop',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op run_after_loop failed with return code {e.returncode}")
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
        context.log.error(f"Op run_after_loop timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="hive_script_task",
    description="Hive Script Task",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def hive_script_task(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Hive Script Task
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: hive_script_task")
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
        
        context.log.info(f"Op hive_script_task completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'hive_script_task',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op hive_script_task failed with return code {e.returncode}")
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
        context.log.error(f"Op hive_script_task timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="run_after_loop_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def run_after_loop_pipeline():
    """
    Job: run_after_loop_pipeline
    
    Pattern: sequential
    Ops: 2
    """
    # Execute ops in order based on dependencies
    
    # Op: run_after_loop
    # Entry point - no dependencies
    run_after_loop_result = run_after_loop()
    
    # Op: hive_script_task
    # Single upstream dependency
    hive_script_task_result = hive_script_task()


# --- Resources ---
from dagster import resource

@resource
def hive_local_resource(context):
    """Resource: hive_local"""
    # TODO: Implement resource initialization
    return {"resource_key": "hive_local"}



# --- Repository ---
from dagster import repository

@repository
def run_after_loop_pipeline_repository():
    """Repository containing run_after_loop_pipeline job."""
    return [run_after_loop_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = run_after_loop_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")