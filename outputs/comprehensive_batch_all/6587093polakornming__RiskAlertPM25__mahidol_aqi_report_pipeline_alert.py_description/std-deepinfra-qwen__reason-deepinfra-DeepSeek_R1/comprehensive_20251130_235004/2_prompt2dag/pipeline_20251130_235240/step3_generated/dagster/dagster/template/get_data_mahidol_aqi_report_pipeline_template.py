# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: get_data_mahidol_aqi_report_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T00:01:27.340823
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
    name="get_data_mahidol_aqi_report",
    description="Scrape Mahidol AQI Report",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def get_data_mahidol_aqi_report(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Scrape Mahidol AQI Report
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: get_data_mahidol_aqi_report")
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
        
        context.log.info(f"Op get_data_mahidol_aqi_report completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'get_data_mahidol_aqi_report',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op get_data_mahidol_aqi_report failed with return code {e.returncode}")
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
        context.log.error(f"Op get_data_mahidol_aqi_report timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="create_json_object",
    description="Parse and Validate AQI Data",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def create_json_object(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Parse and Validate AQI Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: create_json_object")
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
        
        context.log.info(f"Op create_json_object completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'create_json_object',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op create_json_object failed with return code {e.returncode}")
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
        context.log.error(f"Op create_json_object timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="load_mahidol_aqi_to_postgres",
    description="Load AQI Data to PostgreSQL",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def load_mahidol_aqi_to_postgres(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Load AQI Data to PostgreSQL
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: load_mahidol_aqi_to_postgres")
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
        
        context.log.info(f"Op load_mahidol_aqi_to_postgres completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'load_mahidol_aqi_to_postgres',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op load_mahidol_aqi_to_postgres failed with return code {e.returncode}")
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
        context.log.error(f"Op load_mahidol_aqi_to_postgres timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="alert_email",
    description="Send AQI Alert Emails",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def alert_email(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Send AQI Alert Emails
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: alert_email")
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
        
        context.log.info(f"Op alert_email completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'alert_email',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op alert_email failed with return code {e.returncode}")
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
        context.log.error(f"Op alert_email timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="get_data_mahidol_aqi_report_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def get_data_mahidol_aqi_report_pipeline():
    """
    Job: get_data_mahidol_aqi_report_pipeline
    
    Pattern: sequential
    Ops: 4
    """
    # Execute ops in order based on dependencies
    
    # Op: get_data_mahidol_aqi_report
    # Entry point - no dependencies
    get_data_mahidol_aqi_report_result = get_data_mahidol_aqi_report()
    
    # Op: create_json_object
    # Single upstream dependency
    create_json_object_result = create_json_object()
    
    # Op: load_mahidol_aqi_to_postgres
    # Single upstream dependency
    load_mahidol_aqi_to_postgres_result = load_mahidol_aqi_to_postgres()
    
    # Op: alert_email
    # Single upstream dependency
    alert_email_result = alert_email()


# --- Resources ---
from dagster import resource

@resource
def postgres_conn_resource(context):
    """Resource: postgres_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "postgres_conn"}

@resource
def smtp_conn_resource(context):
    """Resource: smtp_conn"""
    # TODO: Implement resource initialization
    return {"resource_key": "smtp_conn"}

@resource
def mahidol_aqi_website_resource(context):
    """Resource: mahidol_aqi_website"""
    # TODO: Implement resource initialization
    return {"resource_key": "mahidol_aqi_website"}



# --- Repository ---
from dagster import repository

@repository
def get_data_mahidol_aqi_report_pipeline_repository():
    """Repository containing get_data_mahidol_aqi_report_pipeline job."""
    return [get_data_mahidol_aqi_report_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = get_data_mahidol_aqi_report_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")