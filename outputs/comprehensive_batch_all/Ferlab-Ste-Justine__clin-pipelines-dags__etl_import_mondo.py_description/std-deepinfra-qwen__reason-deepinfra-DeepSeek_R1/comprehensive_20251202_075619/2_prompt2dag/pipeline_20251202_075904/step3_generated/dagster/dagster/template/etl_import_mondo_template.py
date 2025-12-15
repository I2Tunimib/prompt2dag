# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: etl_import_mondo
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T08:04:25.527249
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
from dagster_k8s import k8s_job_executor

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/dagster/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Op Definitions ---

@op(
    name="params_validate",
    description="Validate Parameters",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def params_validate(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Validate Parameters
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: params_validate")
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
        
        context.log.info(f"Op params_validate completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'params_validate',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op params_validate failed with return code {e.returncode}")
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
        context.log.error(f"Op params_validate timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="download_mondo_terms",
    description="Download Mondo Terms",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def download_mondo_terms(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Download Mondo Terms
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: download_mondo_terms")
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
        
        context.log.info(f"Op download_mondo_terms completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'download_mondo_terms',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op download_mondo_terms failed with return code {e.returncode}")
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
        context.log.error(f"Op download_mondo_terms timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="normalized_mondo_terms",
    description="Normalize Mondo Terms",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def normalized_mondo_terms(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Normalize Mondo Terms
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: normalized_mondo_terms")
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
        
        context.log.info(f"Op normalized_mondo_terms completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'normalized_mondo_terms',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op normalized_mondo_terms failed with return code {e.returncode}")
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
        context.log.error(f"Op normalized_mondo_terms timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="index_mondo_terms",
    description="Index Mondo Terms",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def index_mondo_terms(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Index Mondo Terms
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: index_mondo_terms")
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
        
        context.log.info(f"Op index_mondo_terms completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'index_mondo_terms',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op index_mondo_terms failed with return code {e.returncode}")
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
        context.log.error(f"Op index_mondo_terms timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="publish_mondo",
    description="Publish Mondo Data",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def publish_mondo(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Publish Mondo Data
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: publish_mondo")
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
        
        context.log.info(f"Op publish_mondo completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'publish_mondo',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op publish_mondo failed with return code {e.returncode}")
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
        context.log.error(f"Op publish_mondo timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="slack",
    description="Send Slack Notification",
    out=Out(Dict[str, Any]),
    tags={"pattern": "sequential", "executor": "docker"},
)
def slack(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Send Slack Notification
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: slack")
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
        
        context.log.info(f"Op slack completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'slack',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op slack failed with return code {e.returncode}")
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
        context.log.error(f"Op slack timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="etl_import_mondo",
    description="No description provided.",
    executor_def=k8s_job_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def etl_import_mondo():
    """
    Job: etl_import_mondo
    
    Pattern: sequential
    Ops: 6
    """
    # Execute ops in order based on dependencies
    
    # Op: params_validate
    # Entry point - no dependencies
    params_validate_result = params_validate()
    
    # Op: download_mondo_terms
    # Single upstream dependency
    download_mondo_terms_result = download_mondo_terms()
    
    # Op: normalized_mondo_terms
    # Single upstream dependency
    normalized_mondo_terms_result = normalized_mondo_terms()
    
    # Op: index_mondo_terms
    # Single upstream dependency
    index_mondo_terms_result = index_mondo_terms()
    
    # Op: publish_mondo
    # Single upstream dependency
    publish_mondo_result = publish_mondo()
    
    # Op: slack
    # Single upstream dependency
    slack_result = slack()


# --- Resources ---
from dagster import resource

@resource
def internal_parameter_validation_system_resource(context):
    """Resource: internal_parameter_validation_system"""
    # TODO: Implement resource initialization
    return {"resource_key": "internal_parameter_validation_system"}

@resource
def k8s_context_resource(context):
    """Resource: k8s_context"""
    # TODO: Implement resource initialization
    return {"resource_key": "k8s_context"}

@resource
def github_releases_resource(context):
    """Resource: github_releases"""
    # TODO: Implement resource initialization
    return {"resource_key": "github_releases"}

@resource
def s3_conn_id_resource(context):
    """Resource: s3_conn_id"""
    # TODO: Implement resource initialization
    return {"resource_key": "s3_conn_id"}

@resource
def internal_publishing_system_resource(context):
    """Resource: internal_publishing_system"""
    # TODO: Implement resource initialization
    return {"resource_key": "internal_publishing_system"}

@resource
def es_url_resource(context):
    """Resource: es_url"""
    # TODO: Implement resource initialization
    return {"resource_key": "es_url"}

@resource
def slack_webhook_resource(context):
    """Resource: slack_webhook"""
    # TODO: Implement resource initialization
    return {"resource_key": "slack_webhook"}



# --- Repository ---
from dagster import repository

@repository
def etl_import_mondo_repository():
    """Repository containing etl_import_mondo job."""
    return [etl_import_mondo]


if __name__ == "__main__":
    # Execute job locally for testing
    result = etl_import_mondo.execute_in_process()
    print(f"Job execution result: {result.success}")