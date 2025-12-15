from __future__ import annotations

import logging
from typing import Any, Dict

import requests
from dagster import InitResourceContext, RetryPolicy, ResourceDefinition, job, op

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MagentoResource:
    """Simple resource for executing GraphQL calls against a Magento endpoint."""

    def __init__(self, endpoint: str, admin_token: str):
        self.endpoint = endpoint.rstrip("/")
        self.admin_token = admin_token

    def execute(self, query: str, variables: Dict[str, Any] = None, token: str | None = None) -> Dict[str, Any]:
        """Execute a GraphQL request.

        Args:
            query: GraphQL query or mutation string.
            variables: Variables for the GraphQL operation.
            token: Optional bearer token for the request (e.g., customer token). If omitted,
                the admin token configured for the resource is used.

        Returns:
            Parsed JSON response.

        Raises:
            requests.HTTPError: If the request fails.
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token or self.admin_token}",
        }
        payload = {"query": query, "variables": variables or {}}
        response = requests.post(self.endpoint, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        json_resp = response.json()
        if "errors" in json_resp:
            raise RuntimeError(f"GraphQL errors: {json_resp['errors']}")
        return json_resp.get("data", {})


def magento_resource(init_context: InitResourceContext) -> MagentoResource:
    """Factory for MagentoResource.

    Expected config:
        {
            "endpoint": "https://magento.example.com/graphql",
            "admin_token": "admin-secret-token"
        }
    """
    config = init_context.resource_config
    return MagentoResource(endpoint=config["endpoint"], admin_token=config["admin_token"])


@op(
    retry_policy=RetryPolicy(max_retries=1, delay=300),
    description="Create a new Magento customer via GraphQL mutation.",
    required_resource_keys={"magento"},
)
def create_customer(context) -> str:
    """Creates a customer and returns the email address."""
    mutation = """
    mutation CreateCustomer($input: CustomerInput!) {
      createCustomer(input: $input) {
        email
      }
    }
    """
    # Example static customer data; in a real pipeline this would be configurable.
    variables = {
        "input": {
            "firstname": "John",
            "lastname": "Doe",
            "email": "john.doe@example.com",
            "password": "SecurePass123!",
        }
    }
    try:
        result = context.resources.magento.execute(mutation, variables)
        email = result["createCustomer"]["email"]
        context.log.info(f"Customer created with email: {email}")
        return email
    except Exception as exc:
        context.log.error(f"Failed to create customer: {exc}")
        raise


@op(
    retry_policy=RetryPolicy(max_retries=1, delay=300),
    description="Generate a customer authentication token using the email.",
    required_resource_keys={"magento"},
)
def generate_customer_token(context, email: str) -> str:
    """Generates a bearer token for the given customer email."""
    mutation = """
    mutation GenerateToken($email: String!, $password: String!) {
      generateCustomerToken(email: $email, password: $password) {
        token
      }
    }
    """
    # Password must match the one used during creation.
    variables = {"email": email, "password": "SecurePass123!"}
    try:
        result = context.resources.magento.execute(mutation, variables)
        token = result["generateCustomerToken"]["token"]
        context.log.info(f"Generated token for {email}")
        return token
    except Exception as exc:
        context.log.error(f"Failed to generate token for {email}: {exc}")
        raise


@op(
    retry_policy=RetryPolicy(max_retries=1, delay=300),
    description="Retrieve customer profile information using the bearer token.",
    required_resource_keys={"magento"},
)
def get_customer_information(context, token: str) -> Dict[str, Any]:
    """Fetches detailed customer information."""
    query = """
    query {
      customer {
        firstname
        lastname
        email
        addresses {
          default_shipping
          default_billing
          street
          city
          region {
            region
            region_code
          }
          postcode
          country_code
          telephone
        }
      }
    }
    """
    try:
        result = context.resources.magento.execute(query, token=token)
        customer_info = result["customer"]
        context.log.info(f"Retrieved customer information for {customer_info.get('email')}")
        return customer_info
    except Exception as exc:
        context.log.error(f"Failed to retrieve customer information: {exc}")
        raise


@job(
    resource_defs={"magento": ResourceDefinition.resource(magento_resource)},
    description="Magento customer onboarding and profile retrieval pipeline.",
)
def magento_customer_job():
    email = create_customer()
    token = generate_customer_token(email)
    get_customer_information(token)


if __name__ == "__main__":
    # Example configuration; replace with real endpoint and admin token.
    run_config = {
        "resources": {
            "magento": {
                "config": {
                    "endpoint": "https://magento.example.com/graphql",
                    "admin_token": "admin-secret-token",
                }
            }
        }
    }
    result = magento_customer_job.execute_in_process(run_config=run_config)
    if result.success:
        logger.info("Pipeline completed successfully.")
    else:
        logger.error("Pipeline failed.")