# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: check_and_download_ensembl_files_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T07:38:32.727365
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
    name="check_and_download_ensembl_files",
    description="Check and Download Ensembl Files",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def check_and_download_ensembl_files(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Check and Download Ensembl Files
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: check_and_download_ensembl_files")
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
        
        context.log.info(f"Op check_and_download_ensembl_files completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'check_and_download_ensembl_files',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op check_and_download_ensembl_files failed with return code {e.returncode}")
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
        context.log.error(f"Op check_and_download_ensembl_files timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="process_ensembl_files_with_spark",
    description="Process Ensembl Files with Spark",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def process_ensembl_files_with_spark(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Process Ensembl Files with Spark
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: process_ensembl_files_with_spark")
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
        
        context.log.info(f"Op process_ensembl_files_with_spark completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'process_ensembl_files_with_spark',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op process_ensembl_files_with_spark failed with return code {e.returncode}")
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
        context.log.error(f"Op process_ensembl_files_with_spark timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="check_and_download_ensembl_files_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def check_and_download_ensembl_files_pipeline():
    """
    Job: check_and_download_ensembl_files_pipeline
    
    Pattern: sequential
    Ops: 2
    """
    # Execute ops in order based on dependencies
    
    # Op: check_and_download_ensembl_files
    # Entry point - no dependencies
    check_and_download_ensembl_files_result = check_and_download_ensembl_files()
    
    # Op: process_ensembl_files_with_spark
    # Single upstream dependency
    process_ensembl_files_with_spark_result = process_ensembl_files_with_spark()


# --- Resources ---
from dagster import resource

@resource
def config_s3_conn_id_resource(context):
    """Resource: config_s3_conn_id"""
    # TODO: Implement resource initialization
    return {"resource_key": "config_s3_conn_id"}



# --- Repository ---
from dagster import repository

@repository
def check_and_download_ensembl_files_pipeline_repository():
    """Repository containing check_and_download_ensembl_files_pipeline job."""
    return [check_and_download_ensembl_files_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = check_and_download_ensembl_files_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")