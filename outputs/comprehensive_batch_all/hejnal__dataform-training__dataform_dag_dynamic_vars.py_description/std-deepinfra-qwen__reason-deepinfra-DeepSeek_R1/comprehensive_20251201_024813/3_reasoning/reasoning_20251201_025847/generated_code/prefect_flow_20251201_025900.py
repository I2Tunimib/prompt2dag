from prefect import flow, task
from datetime import datetime
from typing import Any, Optional
import time

@task
def start_pipeline():
    """Dummy task to mark pipeline start"""
    print("Pipeline starting")

@task
def parse_parameters(logical_date: datetime, description: str = "default", 
                    git_commitish: str = "main") -> dict:
    """Parse and process runtime parameters"""
    config = {
        "compilation": {
            "git_commitish": git_commitish,
            "logical_date": logical_date.isoformat(),
            "description": description
        }
    }
    print(f"Parsed parameters: {config}")
    return config

@task
def create_compilation_result(config: dict) -> str:
    """Execute Dataform compilation using provided configuration"""
    compilation_id = f"compilation_{int(time.time())}"
    print(f"Created compilation result: {compilation_id}")
    return compilation_id

@task
def create_workflow_invocation(compilation_id: str) -> str:
    """Trigger Dataform workflow execution"""
    invocation_id = f"invocation_{int(time.time())}"
    print(f"Created workflow invocation: {invocation_id}")
    return invocation_id

@task
def monitor_workflow_invocation(invocation_id: str) -> bool:
    """Monitor workflow invocation until completion"""
    print(f"Monitoring invocation {invocation_id}")
    time.sleep(2)  # Simulate polling
    print("Workflow invocation succeeded")
    return True

@task
def end_pipeline():
    """Dummy task to mark pipeline completion"""
    print("Pipeline completed successfully")

@flow
def dataform_transformation_pipeline(
    logical_date: datetime = datetime.now(),
    description: str = "daily-run",
    git_commitish: str = "main"
):
    start_pipeline()
    config = parse_parameters(logical_date, description, git_commitish)
    compilation_id = create_compilation_result(config)
    invocation_id = create_workflow_invocation(compilation_id)
    success = monitor_workflow_invocation(invocation_id)
    
    if success:
        end_pipeline()
    else:
        raise RuntimeError("Workflow invocation failed")

if __name__ == "__main__":
    # Example parameters with default values
    dataform_transformation_pipeline(
        logical_date=datetime(2023, 1, 1),
        description="training-data-transform",
        git_commitish="v1.0"
    )