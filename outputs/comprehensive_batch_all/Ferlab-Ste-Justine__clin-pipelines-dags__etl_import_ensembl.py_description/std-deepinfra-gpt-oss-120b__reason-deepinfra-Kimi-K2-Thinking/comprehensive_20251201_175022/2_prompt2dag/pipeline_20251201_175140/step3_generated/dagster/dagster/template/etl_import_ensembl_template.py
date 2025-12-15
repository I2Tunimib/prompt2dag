# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: etl_import_ensembl
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T17:55:25.742462
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
    name="extract_ensembl_files",
    description="Extract Ensembl Mapping Files",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def extract_ensembl_files(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Extract Ensembl Mapping Files
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: extract_ensembl_files")
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
        
        context.log.info(f"Op extract_ensembl_files completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'extract_ensembl_files',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op extract_ensembl_files failed with return code {e.returncode}")
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
        context.log.error(f"Op extract_ensembl_files timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="transform_ensembl_mapping",
    description="Transform Ensembl Mapping with Spark",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def transform_ensembl_mapping(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Transform Ensembl Mapping with Spark
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: transform_ensembl_mapping")
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
        
        context.log.info(f"Op transform_ensembl_mapping completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'transform_ensembl_mapping',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op transform_ensembl_mapping failed with return code {e.returncode}")
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
        context.log.error(f"Op transform_ensembl_mapping timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="etl_import_ensembl",
    description="Comprehensive Pipeline Description",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def etl_import_ensembl():
    """
    Job: etl_import_ensembl
    
    Pattern: sequential
    Ops: 2
    """
    # Execute ops in order based on dependencies
    
    # Op: extract_ensembl_files
    # Entry point - no dependencies
    extract_ensembl_files_result = extract_ensembl_files()
    
    # Op: transform_ensembl_mapping
    # Single upstream dependency
    transform_ensembl_mapping_result = transform_ensembl_mapping()


# --- Resources ---
from dagster import resource

@resource
def ftp_ensembl_conn_resource(context):
    """Resource: ftp_ensembl_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "ftp_ensembl_conn"}

@resource
def config_s3_conn_id_resource(context):
    """Resource: config_s3_conn_id"""
    # TODO: Implement resource initialization
    return {"resource_key": "config_s3_conn_id"}



# --- Repository ---
from dagster import repository

@repository
def etl_import_ensembl_repository():
    """Repository containing etl_import_ensembl job."""
    return [etl_import_ensembl]


if __name__ == "__main__":
    # Execute job locally for testing
    result = etl_import_ensembl.execute_in_process()
    print(f"Job execution result: {result.success}")