import os
import json
import logging
from typing import Any, Dict

import requests
from prefect import flow, task
from prefect.logging import get_logger

# Configure module level logger
logging.basicConfig(level=logging.INFO)
logger = get_logger(__name__)

MAGENTO_ENDPOINT = os.getenv(
    "MAGENTO_GRAPHQL_ENDPOINT", "https://example.com/graphql"
)


def _post_graphql(query: str, variables: Dict[str, Any] = None, headers: Dict[str, str] = None) -> Dict[str, Any]:
    """Helper to send a GraphQL request to Magento."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)

    response = requests.post(
        MAGENTO_ENDPOINT,
        data=json.dumps(payload),
        headers=request_headers,
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    if "errors" in result:
        raise RuntimeError(f"GraphQL errors: {result['errors']}")
    return result.get("data", {})


@task(retries=1, retry_delay_seconds=300)
def create_customer() -> str:
    """
    Creates a new Magento customer using a GraphQL mutation.

    Returns:
        The email address of the created customer.
    """
    logger.info("Creating Magento customer...")
    mutation = """
    mutation CreateCustomer($input: CustomerInput!) {
      createCustomer(input: $input) {
        customer {
          email
        }
      }
    }
    """
    variables = {
        "input": {
            "email": "test_user@example.com",
            "firstname": "John",
            "lastname": "Doe",
            "password": "Password123!"
        }
    }

    try:
        data = _post_graphql(mutation, variables)
        email = data["createCustomer"]["customer"]["email"]
        logger.info("Customer created with email: %s", email)
        return email
    except Exception as exc:
        logger.error("Failed to create customer: %s", exc)
        raise


@task(retries=1, retry_delay_seconds=300)
def generate_customer_token(email: str) -> str:
    """
    Generates an authentication token for the given customer email.

    Args:
        email: Customer email address.

    Returns:
        Bearer token string.
    """
    logger.info("Generating token for customer %s...", email)
    mutation = """
    mutation GenerateToken($email: String!, $password: String!) {
      generateCustomerToken(email: $email, password: $password) {
        token
      }
    }
    """
    variables = {
        "email": email,
        "password": "Password123!"
    }

    try:
        data = _post_graphql(mutation, variables)
        token = data["generateCustomerToken"]["token"]
        logger.info("Token generated for %s", email)
        return token
    except Exception as exc:
        logger.error("Failed to generate token for %s: %s", email, exc)
        raise


@task(retries=1, retry_delay_seconds=300)
def get_customer_information(token: str) -> Dict[str, Any]:
    """
    Retrieves comprehensive customer profile data using the provided token.

    Args:
        token: Bearer authentication token.

    Returns:
        Dictionary containing customer profile information.
    """
    logger.info("Fetching customer information with token...")
    query = """
    {
      customer {
        firstname
        lastname
        email
        addresses {
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
    headers = {"Authorization": f"Bearer {token}"}

    try:
        data = _post_graphql(query, headers=headers)
        customer_info = data["customer"]
        logger.info("Customer information retrieved.")
        return customer_info
    except Exception as exc:
        logger.error("Failed to retrieve customer information: %s", exc)
        raise


@flow(name="magento_customer_onboarding")
def magento_customer_flow() -> Dict[str, Any]:
    """
    Orchestrates the Magento customer onboarding workflow:
    1. Create a new customer.
    2. Generate an authentication token for the customer.
    3. Retrieve the customer's profile information.
    """
    email = create_customer()
    token = generate_customer_token(email)
    customer_info = get_customer_information(token)
    return customer_info


if __name__ == "__main__":
    # Manual execution; no schedule configured.
    result = magento_customer_flow()
    print(json.dumps(result, indent=2))