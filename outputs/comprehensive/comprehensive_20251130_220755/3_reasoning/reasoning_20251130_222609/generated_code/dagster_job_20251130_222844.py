from dagster import op, job, In, Out, Nothing, Field, Array, String, Dict, Any
import time
import random

# Simulated Kubernetes job execution
def execute_kubernetes_job(job_name, **kwargs):
    """Simulates executing a Kubernetes job."""
    print(f"Executing Kubernetes job: {job_name}")
    time.sleep(2)
    # Simulate random success/failure
    if random.random() > 0.9:
        raise Exception(f"Kubernetes job {job_name} failed")
    return f"{job_name} completed successfully"

# Simulated HTTP API call
def call_http_api(api_name, **kwargs):
    """Simulates calling an HTTP API endpoint."""
    print(f"Calling HTTP API: {api_name}")
    time.sleep(1)
    # Simulate random success/failure
    if random.random() > 0.8:
        raise Exception(f"API call {api_name} failed")
    return f"{api_name} data extracted"