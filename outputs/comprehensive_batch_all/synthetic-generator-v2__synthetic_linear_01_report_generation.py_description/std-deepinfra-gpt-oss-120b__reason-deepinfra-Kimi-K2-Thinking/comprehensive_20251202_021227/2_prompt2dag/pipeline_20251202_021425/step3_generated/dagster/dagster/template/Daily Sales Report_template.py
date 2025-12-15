# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: Daily Sales Report
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T02:17:54.720492
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
    name="query_sales_data",
    description="Query Sales Data",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def query_sales_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Query Sales Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: query_sales_data")
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
        
        context.log.info(f"Op query_sales_data completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'query_sales_data',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op query_sales_data failed with return code {e.returncode}")
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
        context.log.error(f"Op query_sales_data timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="transform_to_csv",
    description="Transform Sales Data to CSV",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def transform_to_csv(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Transform Sales Data to CSV
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: transform_to_csv")
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
        
        context.log.info(f"Op transform_to_csv completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'transform_to_csv',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op transform_to_csv failed with return code {e.returncode}")
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
        context.log.error(f"Op transform_to_csv timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="generate_pdf_chart",
    description="Generate PDF Chart",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def generate_pdf_chart(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Generate PDF Chart
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: generate_pdf_chart")
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
        
        context.log.info(f"Op generate_pdf_chart completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'generate_pdf_chart',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op generate_pdf_chart failed with return code {e.returncode}")
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
        context.log.error(f"Op generate_pdf_chart timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="email_sales_report",
    description="Email Sales Report",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def email_sales_report(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Email Sales Report
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: email_sales_report")
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
        
        context.log.info(f"Op email_sales_report completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'email_sales_report',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op email_sales_report failed with return code {e.returncode}")
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
        context.log.error(f"Op email_sales_report timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="daily_sales_report",
    description="Sequential linear pipeline that generates daily sales reports by querying PostgreSQL, converting to CSV, creating a PDF chart, and emailing the report.",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def daily_sales_report():
    """
    Job: Daily Sales Report
    
    Pattern: sequential
    Ops: 4
    """
    # Execute ops in order based on dependencies
    
    # Op: query_sales_data
    # Entry point - no dependencies
    query_sales_data_result = query_sales_data()
    
    # Op: transform_to_csv
    # Single upstream dependency
    transform_to_csv_result = transform_to_csv()
    
    # Op: generate_pdf_chart
    # Single upstream dependency
    generate_pdf_chart_result = generate_pdf_chart()
    
    # Op: email_sales_report
    # Single upstream dependency
    email_sales_report_result = email_sales_report()


# --- Resources ---
from dagster import resource

@resource
def postgres_default_resource(context):
    """Resource: postgres_default"""
    # TODO: Implement resource initialization
    return {"resource_key": "postgres_default"}

@resource
def email_smtp_resource(context):
    """Resource: email_smtp"""
    # TODO: Implement resource initialization
    return {"resource_key": "email_smtp"}

@resource
def local_fs_resource(context):
    """Resource: local_fs"""
    # TODO: Implement resource initialization
    return {"resource_key": "local_fs"}



# --- Repository ---
from dagster import repository

@repository
def daily_sales_report_repository():
    """Repository containing daily_sales_report job."""
    return [daily_sales_report]


if __name__ == "__main__":
    # Execute job locally for testing
    result = daily_sales_report.execute_in_process()
    print(f"Job execution result: {result.success}")