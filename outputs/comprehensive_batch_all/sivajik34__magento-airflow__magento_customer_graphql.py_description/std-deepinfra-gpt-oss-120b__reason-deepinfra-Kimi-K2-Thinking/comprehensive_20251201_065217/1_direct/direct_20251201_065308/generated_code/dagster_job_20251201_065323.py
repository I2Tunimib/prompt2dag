import requests
from typing import Any, Dict

from dagster import (
    InitResourceContext,
    RetryPolicy,
    ResourceDefinition,
    job,
    op,
)


class MagentoGraphQLClient:
    """Simple client for executing Magento GraphQL queries."""

    def __init__(self, endpoint: str, admin_token: str):
        self.endpoint = endpoint
        self.admin_token = admin_token

    def execute(
        self, query: str, variables: Dict[str, Any] = None, token: str = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL query against the Magento endpoint."""
        headers = {"Content-Type": "application/json"}
        auth_token = token if token else self.admin_token
        headers["Authorization"] = f"Bearer {auth_token}"

        payload = {"query": query, "variables": variables or {}}
        response = requests.post(self.endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()


def magento_resource(init_context: InitResourceContext) -> MagentoGraphQLClient:
    """Dagster resource that provides a MagentoGraphQLClient."""
    config = init_context.resource_config
    return MagentoGraphQLClient(
        endpoint=config["endpoint"], admin_token=config["admin_token"]
    )


magento_resource_def = ResourceDefinition(
    resource_fn=magento_resource,
    config_schema={"endpoint": str, "admin_token": str},
    description="Magento GraphQL API client",
)


@op(
    retry_policy=RetryPolicy(max_retries=1, delay=300),
    description="Create a new Magento customer and return the email address.",
)
def create_customer(context) -> str:
    query = """
    mutation createCustomer($input: CustomerInput!) {
      createCustomer(input: $input) {
        email
      }
    }
    """
    variables = {
        "input": {
            "firstname": "John",
            "lastname": "Doe",
            "email": "john.doe@example.com",
            "password": "Password123!",
        }
    }
    client: MagentoGraphQLClient = context.resources.magento
    result = client.execute(query, variables)
    email = result["data"]["createCustomer"]["email"]
    context.log.info(f"Created customer with email: {email}")
    return email


@op(
    retry_policy=RetryPolicy(max_retries=1, delay=300),
    description="Generate an authentication token for a customer using their email.",
)
def generate_customer_token(context, email: str) -> str:
    query = """
    mutation generateToken($email: String!, $password: String!) {
      generateCustomerToken(email: $email, password: $password) {
        token
      }
    }
    """
    variables = {"email": email, "password": "Password123!"}
    client: MagentoGraphQLClient = context.resources.magento
    result = client.execute(query, variables)
    token = result["data"]["generateCustomerToken"]["token"]
    context.log.info(f"Generated token for customer {email}")
    return token


@op(
    retry_policy=RetryPolicy(max_retries=1, delay=300),
    description="Retrieve comprehensive customer profile information using a bearer token.",
)
def get_customer_information(context, token: str) -> Dict[str, Any]:
    query = """
    query {
      customer {
        firstname
        lastname
        email
        addresses {
          street
          city
          postcode
          country_code
        }
      }
    }
    """
    client: MagentoGraphQLClient = context.resources.magento
    result = client.execute(query, token=token)
    customer_info = result["data"]["customer"]
    context.log.info(f"Retrieved customer information: {customer_info}")
    return customer_info


@job(resource_defs={"magento": magento_resource_def})
def magento_customer_onboarding():
    """Linear workflow: create customer → generate token → fetch profile."""
    email = create_customer()
    token = generate_customer_token(email)
    get_customer_information(token)


if __name__ == "__main__":
    # Example execution with inline configuration.
    result = magento_customer_onboarding.execute_in_process(
        run_config={
            "resources": {
                "magento": {
                    "config": {
                        "endpoint": "https://magento.example.com/graphql",
                        "admin_token": "YOUR_ADMIN_TOKEN",
                    }
                }
            }
        }
    )
    if result.success:
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")