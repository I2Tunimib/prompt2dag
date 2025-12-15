from dagster import (
    op,
    job,
    Config,
    ResourceParam,
    RetryPolicy,
    OpExecutionContext,
    resource,
)
from typing import Dict, Any
import time
import hashlib
from datetime import datetime


class DataformClient:
    """Minimal stub for Dataform API client."""
    
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
    
    def compile_workflow(self, config: Dict[str, Any]) -> str:
        """Simulate workflow compilation."""
        return f"compilation-result-{hash(str(config))}"
    
    def create_workflow_invocation(self, compilation_result_id: str) -> str:
        """Simulate workflow invocation."""
        return