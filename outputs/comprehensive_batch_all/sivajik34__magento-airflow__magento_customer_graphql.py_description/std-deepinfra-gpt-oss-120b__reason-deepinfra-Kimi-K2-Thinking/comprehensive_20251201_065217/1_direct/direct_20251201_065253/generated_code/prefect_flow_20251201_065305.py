import os
import logging
from typing import Dict, Any

import requests
from prefect import flow, task

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Magento GraphQL endpoint and admin token are expected to be provided via environment variables.
MAGENTO_ENDPOINT = os.getenv(
    "MAGENTO_GRAPHQL_ENDPOINT", "https://example.com/graphql"
)
MAGENTO_ADMIN_TOKEN = os.getenv("MAGENTO_ADMIN_TOKEN", "")


@task(retries=1, retry_delay_seconds=300)
def create_customer() -> str:
    """
    Create a new Magento customer via GraphQL mutation.

    Returns:
        The email address of the created customer.
    """
    mutation = """
    mutation CreateCustomer(
        $email: String!
        $firstname: String!
        $lastname: String!
        $password: String!
    ) {
      createCustomer(
        input: {
          email: $email
          firstname: $firstname
          lastname: $lastname
          password: $password
        }
      ) {
        customer {
          email
        }
      }
    }
    """
    variables = {
        "email": "newcustomer@example.com",
        "firstname": "John",
        "lastname": "Doe",
        "password": "Secret123!",
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MAGENTO_ADMIN_TOKEN}",
    }

    try:
        response = requests.post(
            MAGENTO_ENDPOINT,
            json={"query": mutation, "variables": variables},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        email = payload["data"]["createCustomer"]["customer"]["email"]
        logger.info("Customer created with email: %s", email)
        return email
    except Exception as exc:
        logger.exception("Failed to create customer: %s", exc)
        raise


@task(retries=1, retry_delay_seconds=300)
def generate_customer_token(email: str) -> str:
    """
    Generate an authentication token for the given customer email.

    Args:
        email: The customer's email address.

    Returns:
        The bearer token string.
    """
    mutation = """
    mutation GenerateToken($email: String!, $password: String!) {
      generateCustomerToken(email: $email, password: $password) {
        token
      }
    }
    """
    # The password must match the one used during creation.
    variables = {"email": email, "password": "Secret123!"}
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            MAGENTO_ENDPOINT,
            json={"query": mutation, "variables": variables},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload["data"]["generateCustomerToken"]["token"]
        logger.info("Generated token for customer %s", email)
        return token
    except Exception as exc:
        logger.exception("Failed to generate token for %s: %s", email, exc)
        raise


@task(retries=1, retry_delay_seconds=300)
def get_customer_information(token: str) -> Dict[str, Any]:
    """
    Retrieve detailed customer information using the provided bearer token.

    Args:
        token: Authentication bearer token.

    Returns:
        A dictionary containing the customer's profile data.
    """
    query = """
    query {
      customer {
        firstname
        lastname
        email
        addresses {
          street
          city
          region
          postcode
          country_code
        }
      }
    }
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.post(
            MAGENTO_ENDPOINT,
            json={"query": query},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        customer_data = payload["data"]["customer"]
        logger.info("Retrieved customer information for %s", customer_data.get("email"))
        return customer_data
    except Exception as exc:
        logger.exception("Failed to retrieve customer information: %s", exc)
        raise


@flow(name="magento_customer_onboarding")
def magento_customer_flow() -> Dict[str, Any]:
    """
    Orchestrates the Magento customer onboarding workflow:
    1. Create a customer.
    2. Generate an authentication token.
    3. Retrieve the customer's profile information.
    """
    email = create_customer()
    token = generate_customer_token(email)
    customer_info = get_customer_information(token)
    return customer_info


if __name__ == "__main__":
    # Manual execution; for scheduled runs, configure a Prefect deployment with the desired schedule.
    result = magento_customer_flow()
    print("Customer information retrieved:", result)