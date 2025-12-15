# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: Airflow Database Cleanup
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T05:24:48.627348
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
    name="extract_cleanup_configuration",
    description="Extract Cleanup Configuration",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extract_cleanup_configuration(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Extract Cleanup Configuration
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extract_cleanup_configuration")
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
        
        context.log.info(f"Op extract_cleanup_configuration completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extract_cleanup_configuration',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extract_cleanup_configuration failed with return code {e.returncode}")
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
        context.log.error(f"Op extract_cleanup_configuration timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="cleanup_airflow_metadb",
    description="Cleanup Airflow MetaDB",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=1,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def cleanup_airflow_metadb(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Cleanup Airflow MetaDB
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: cleanup_airflow_metadb")
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
        
        context.log.info(f"Op cleanup_airflow_metadb completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'cleanup_airflow_metadb',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op cleanup_airflow_metadb failed with return code {e.returncode}")
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
        context.log.error(f"Op cleanup_airflow_metadb timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="airflow_database_cleanup",
    description="Maintenance workflow that periodically cleans old metadata entries from Airflow's MetaStore database tables to prevent excessive data accumulation.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def airflow_database_cleanup():
    """
    Job: Airflow Database Cleanup
    
    Pattern: sequential
    Ops: 2
    """
    # Execute ops in order based on dependencies
    
    # Op: extract_cleanup_configuration
    # Entry point - no dependencies
    extract_cleanup_configuration_result = extract_cleanup_configuration()
    
    # Op: cleanup_airflow_metadb
    # Single upstream dependency
    cleanup_airflow_metadb_result = cleanup_airflow_metadb()


# --- Resources ---
from dagster import resource

@resource
def airflow_default_db_resource(context):
    """Resource: airflow_default_db"""
    # TODO: Implement resource initialization
    return {"resource_key": "airflow_default_db"}



# --- Repository ---
from dagster import repository

@repository
def airflow_database_cleanup_repository():
    """Repository containing airflow_database_cleanup job."""
    return [airflow_database_cleanup]


if __name__ == "__main__":
    # Execute job locally for testing
    result = airflow_database_cleanup.execute_in_process()
    print(f"Job execution result: {result.success}")