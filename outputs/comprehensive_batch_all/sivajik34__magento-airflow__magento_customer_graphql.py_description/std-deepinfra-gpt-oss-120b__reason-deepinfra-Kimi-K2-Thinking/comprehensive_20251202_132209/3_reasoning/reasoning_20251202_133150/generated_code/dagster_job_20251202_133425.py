from dagster import (
    op,
    job,
    Out,
    In,
    RetryPolicy,
    Field,
    resource,
    StringSource,
    IntSource,
    OpExecutionContext,
)
import logging
from typing import Dict, Any


class MagentoGraphQLClient:
    """Simplified Magento GraphQL client stub."""
    
    def __init__(self, endpoint: str, timeout: int = 30):
        self.endpoint = endpoint
        self.timeout = timeout
    
    def execute_mutation(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a GraphQL mutation."""
        logging.info(f"Executing mutation on {self.endpoint}")
        
        if "createCustomer" in query:
            return {
                "data": {
                    "createCustomer": {
                        "customer": {"email": variables["input"]["email"]}
                    }
                }
            }
        elif "generateCustomerToken" in query:
            return