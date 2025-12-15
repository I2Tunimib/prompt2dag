import os
from typing import Dict, Any
from prefect import flow, task, get_run_logger
import requests


def get_magento_config() -> Dict[str, str]:
    """Retrieve Magento configuration from environment or Prefect Blocks."""
    return {
        "graphql_endpoint": os.getenv(
            "MAGENTO_GRAPHQL_ENDPOINT",
            "https://magento.example.com/graphql"
        )
    }


@task(
    name="create_customer",
    description="Create a new Magento customer account via GraphQL mutation",
    retries=1,
    retry_delay_seconds=300
)
def create_customer(
    firstname: str,
    lastname: str,
    email: str,
    password: str
) -> str:
    """Create a new customer and return their email address."""
    logger = get_run_logger()
    config = get_magento_config()
    
    mutation = """
    mutation createCustomer($input: CustomerInput!) {
        createCustomer(input: $input) {
            customer {
                email
            }
        }
    }
    """
    
    try:
        logger.info(f"Creating customer with email: {email}")
        response = requests.post(
            config["graphql_endpoint"],
            json={
                "query": mutation,
                "variables": {
                    "input": {
                        "firstname": firstname,
                        "lastname": lastname,
                        "email": email,
                        "password": password
                    }
                }
            },
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if errors := data.get("errors"):
            raise Exception(f"GraphQL errors: {errors}")
        
        created_email = data["data"]["createCustomer"]["customer"]["email"]
        logger.info(f"Successfully created customer: {created_email}")
        return created_email
        
    except Exception as exc:
        logger.error(f"Failed to create customer: {exc}")
        raise


@task(
    name="generate_customer_token",
    description="Generate authentication token for a customer",
    retries=1,
    retry_delay_seconds=300
)
def generate_customer_token(email: str, password: str) -> str:
    """Generate and return a customer authentication token."""
    logger = get_run_logger()
    config = get_magento_config()
    
    mutation = """
    mutation generateCustomerToken($email: String!, $password: String!) {
        generateCustomerToken(email: $email, password: $password) {
            token
        }
    }
    """
    
    try:
        logger.info(f"Generating token for customer: {email}")
        response = requests.post(
            config["graphql_endpoint"],
            json={
                "query": mutation,
                "variables": {"email": email, "password": password}
            },
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if errors := data.get("errors"):
            raise Exception(f"GraphQL errors: {errors}")
        
        token = data["data"]["generateCustomerToken"]["token"]
        logger.info("Successfully generated customer token")
        return token
        
    except Exception as exc:
        logger.error(f"Failed to generate token for {email}: {exc}")
        raise


@task(
    name="get_customer_information",
    description="Retrieve customer profile information using authentication token",
    retries=1,
    retry_delay_seconds=300
)
def get_customer_information(token: str) -> Dict[str, Any]:
    """Retrieve and return comprehensive customer profile data."""
    logger = get_run_logger()
    config = get_magento_config()
    
    query = """
    query getCustomerInfo {
        customer {
            firstname
            lastname
            email
            addresses {
                firstname
                lastname
                street
                city
                region
                postcode
                country_code
            }
        }
    }
    """
    
    try:
        logger.info("Retrieving customer information")
        response = requests.post(
            config["graphql_endpoint"],
            json={"query": query},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}"
            },
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if errors := data.get("errors"):
            raise Exception(f"GraphQL errors: {errors}")
        
        customer_info = data["data"]["customer"]
        logger.info(f"Successfully retrieved info for: {customer_info['email']}")
        return customer_info
        
    except Exception as exc:
        logger.error(f"Failed to retrieve customer information: {exc}")
        raise


@flow(
    name="magento-customer-management-pipeline",
    description="Sequential Magento customer management operations using GraphQL",
    retries=1,
    retry_delay_seconds=300
)
def magento_customer_pipeline(
    firstname: str = "John",
    lastname: str = "Doe",
    email: str = "john.doe@example.com",
    password: str = "SecurePassword123!"
) -> Dict[str, Any]:
    """
    Orchestrate the Magento customer management pipeline.
    
    Linear workflow: Create Customer → Generate Token → Get Information
    """
    logger = get_run_logger()
    logger.info("Starting Magento customer management pipeline")
    
    # Step 1: Create customer account
    customer_email = create_customer(
        firstname=firstname,
        lastname=lastname,
        email=email,
        password=password
    )
    
    # Step 2: Generate authentication token
    auth_token = generate_customer_token(
        email=customer_email,
        password=password
    )
    
    # Step 3: Retrieve customer profile
    profile = get_customer_information(token=auth_token)
    
    logger.info("Pipeline completed successfully")
    return profile


if __name__ == "__main__":
    # Manual execution only - no schedule configured
    # Deploy with: prefect deployment build magento_customer_pipeline.py:magento_customer_pipeline --name manual-run
    magento_customer_pipeline()