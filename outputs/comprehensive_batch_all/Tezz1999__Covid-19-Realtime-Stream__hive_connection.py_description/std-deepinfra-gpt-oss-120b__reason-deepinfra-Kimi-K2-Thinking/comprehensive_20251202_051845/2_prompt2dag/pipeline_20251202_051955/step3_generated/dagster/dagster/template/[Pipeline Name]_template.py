# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: [Pipeline Name]
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T05:23:10.872242
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
    name="run_system_check",
    description="Run System Check",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def run_system_check(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Run System Check
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: run_system_check")
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
        
        context.log.info(f"Op run_system_check completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'run_system_check',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op run_system_check failed with return code {e.returncode}")
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
        context.log.error(f"Op run_system_check timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="run_hive_script",
    description="Run Hive Script",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def run_hive_script(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Run Hive Script
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: run_hive_script")
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
        
        context.log.info(f"Op run_hive_script completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'run_hive_script',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op run_hive_script failed with return code {e.returncode}")
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
        context.log.error(f"Op run_hive_script timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="[pipeline_name]",
    description="Simple linear data pipeline that executes Hive database operations for COVID-19 realtime streaming data.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def [pipeline_name]():
    """
    Job: [Pipeline Name]
    
    Pattern: sequential
    Ops: 2
    """
    # Execute ops in order based on dependencies
    
    # Op: run_system_check
    # Entry point - no dependencies
    run_system_check_result = run_system_check()
    
    # Op: run_hive_script
    # Single upstream dependency
    run_hive_script_result = run_hive_script()


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
def [pipeline_name]_repository():
    """Repository containing [pipeline_name] job."""
    return [[pipeline_name]]


if __name__ == "__main__":
    # Execute job locally for testing
    result = [pipeline_name].execute_in_process()
    print(f"Job execution result: {result.success}")