# ==============================================================================
# Generated Dagster Job - Sequential Pattern
# Pipeline: fetch_brokerage_holdings_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T19:18:42.936854
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
    name="fetch_brokerage_holdings",
    description="Fetch Brokerage Holdings",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def fetch_brokerage_holdings(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Fetch Brokerage Holdings
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: fetch_brokerage_holdings")
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
        
        context.log.info(f"Op fetch_brokerage_holdings completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'fetch_brokerage_holdings',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op fetch_brokerage_holdings failed with return code {e.returncode}")
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
        context.log.error(f"Op fetch_brokerage_holdings timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="analyze_portfolio",
    description="Analyze Portfolio",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def analyze_portfolio(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Analyze Portfolio
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: analyze_portfolio")
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
        
        context.log.info(f"Op analyze_portfolio completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'analyze_portfolio',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op analyze_portfolio failed with return code {e.returncode}")
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
        context.log.error(f"Op analyze_portfolio timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="aggregate_and_rebalance",
    description="Aggregate and Rebalance",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def aggregate_and_rebalance(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Aggregate and Rebalance
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: aggregate_and_rebalance")
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
        
        context.log.info(f"Op aggregate_and_rebalance completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'aggregate_and_rebalance',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op aggregate_and_rebalance failed with return code {e.returncode}")
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
        context.log.error(f"Op aggregate_and_rebalance timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


@op(
    name="generate_trade_orders",
    description="Generate Trade Orders",
    out=Out(Dict[str, Any]),
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    tags={"pattern": "sequential", "executor": "docker"},
)
def generate_trade_orders(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Op: Generate Trade Orders
    
    Executes Docker container: 
    """
    context.log.info(f"Starting op: generate_trade_orders")
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
        
        context.log.info(f"Op generate_trade_orders completed successfully")
        if result.stdout:
            context.log.debug(f"STDOUT: {result.stdout}")
        
        return {
            'op_id': 'generate_trade_orders',
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode,
        }
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Op generate_trade_orders failed with return code {e.returncode}")
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
        context.log.error(f"Op generate_trade_orders timed out")
        raise Failure(
            description=f"Docker container timed out after 3600s",
        )


# --- Job Definition ---
@job(
    name="fetch_brokerage_holdings_pipeline",
    description="Portfolio Rebalancing DAG",
    executor_def=in_process_executor,
    tags={"pattern": "sequential", "generated": "template"},
)
def fetch_brokerage_holdings_pipeline():
    """
    Job: fetch_brokerage_holdings_pipeline
    
    Pattern: sequential
    Ops: 4
    """
    # Execute ops in order based on dependencies
    
    # Op: fetch_brokerage_holdings
    # Entry point - no dependencies
    fetch_brokerage_holdings_result = fetch_brokerage_holdings()
    
    # Op: analyze_portfolio
    # Single upstream dependency
    analyze_portfolio_result = analyze_portfolio()
    
    # Op: aggregate_and_rebalance
    # Single upstream dependency
    aggregate_and_rebalance_result = aggregate_and_rebalance()
    
    # Op: generate_trade_orders
    # Single upstream dependency
    generate_trade_orders_result = generate_trade_orders()


# --- Resources ---


# --- Repository ---
from dagster import repository

@repository
def fetch_brokerage_holdings_pipeline_repository():
    """Repository containing fetch_brokerage_holdings_pipeline job."""
    return [fetch_brokerage_holdings_pipeline]


if __name__ == "__main__":
    # Execute job locally for testing
    result = fetch_brokerage_holdings_pipeline.execute_in_process()
    print(f"Job execution result: {result.success}")