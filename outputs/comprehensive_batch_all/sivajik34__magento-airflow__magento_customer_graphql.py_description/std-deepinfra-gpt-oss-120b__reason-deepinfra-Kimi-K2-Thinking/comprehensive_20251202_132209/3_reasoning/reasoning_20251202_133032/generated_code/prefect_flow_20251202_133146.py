import logging
from typing import Dict, Any

import requests
from prefect import flow, task
from prefect.blocks.system import Secret


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(
    retries=1,
    retry_delay_seconds=300,
    name="create_customer",
    description="Create a new Magento customer account via GraphQL mutation"
)
def create_customer(
    firstname: str,
    lastname: str,
    email: str,
    password: str
) -> str:
    """
    Create a new Magento customer account.
    
    Args:
        firstname: Customer first name
        lastname: Customer last name
        email: Customer email
        password: Customer password
        
    Returns:
        str: The created customer's email address
    """
    try:
        magento_config = Secret.load("magento_default").get()
        magento_url = magento_config["url"]
        admin_token = magento_config["admin_token"]
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {admin_token}"
        }
        
        mutation = """
        mutation createCustomer($input: CustomerInput!) {
            createCustomer(input: $input) {
                customer {
                    email
                }
            }
        }
        """
        
        variables = {
            "input": {
                "firstname": firstname,
                "lastname": lastname,
                "email": email,
                "password": password
            }
        }
        
        response = requests.post(
            magento_url,
            json={"query": mutation, "variables": variables},
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        if errors := data.get("errors"):
            raise Exception(f"GraphQL errors: {errors}")
        
        customer_email = data["data"]["createCustomer"]["customer"]["email"]
        logger.info(f"Successfully created customer: {customer_email}")
        return customer_email
        
    except Exception as e:
        logger.error(f"Failed to create customer: {e}")
        raise


@task(
    retries=1,
    retry_delay_seconds=300,
    name="generate_customer_token",
    description="Generate authentication token for the customer"
)
def generate_customer_token(customer_email: str, password: str) -> str:
    """
    Generate authentication token using customer credentials.
    
    Args:
        customer_email: The customer's email address
        password: The customer's password
        
    Returns:
        str: The generated bearer token
    """
    try:
        magento_config = Secret.load("magento_default").get()
        magento_url = magento_config["url"]
        
        mutation = """
        mutation generateCustomerToken($email: String!, $password: String!) {
            generateCustomerToken(email: $email, password: $password) {
                token
            }
        }
        """
        
        variables = {
            "email": customer_email,
            "password": password
        }
        
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(
            magento_url,
            json={"query": mutation, "variables": variables},
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        if errors := data.get("errors"):
            raise Exception(f"GraphQL errors: {errors}")
        
        token = data["data"]["generateCustomerToken"]["token"]
        logger.info(f"Token generated for customer: {customer_email}")
        return token
        
    except Exception as e:
        logger.error(f"Failed to generate token for {customer_email}: {e}")
        raise


@task(
    retries=1,
    retry_delay_seconds=300,
    name="get_customer_information",
    description="Retrieve customer profile information using authentication token"
)
def get_customer_information(token: str) -> Dict[str, Any]:
    """
    Retrieve comprehensive customer profile data.
    
    Args:
        token: The customer's bearer token
        
    Returns:
        Dict[str, Any]: Customer profile data
    """
    try:
        magento_config = Secret.load("magento_default").get()
        magento_url = magento_config["url"]
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        query = """
        query {
            customer {
                firstname
                lastname
                email
                addresses {
                    firstname
                    lastname
                    street
                    city
                    region {
                        region
                    }
                    postcode
                    country_code
                }
            }
        }
        """
        
        response = requests.post(
            magento_url,
            json={"query": query},
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        if errors := data.get("errors"):
            raise Exception(f"GraphQL errors: {errors}")
        
        customer_info = data["data"]["customer"]
        logger.info("Customer information retrieved successfully")
        return customer_info
        
    except Exception as e:
        logger.error(f"Failed to retrieve customer information: {e}")
        raise


@flow(
    name="magento-customer-pipeline",
    description="Sequential Magento customer management operations using GraphQL"
)
def magento_customer_pipeline(
    firstname: str = "John",
    lastname: str = "Doe",
    email: str = "john.doe@example.com",
    password: str = "SecurePassword123!"
) -> Dict[str, Any]:
    """
    Main flow that orchestrates Magento customer operations:
    1. Create customer
    2. Generate token
    3. Get customer information
    
    Args:
        firstname: Customer first name
        lastname: Customer last name
        email: Customer email
        password: Customer password
        
    Returns:
        Dict[str, Any]: Final customer information
    """
    customer_email = create_customer(firstname, lastname, email, password)
    token = generate_customer_token(customer_email, password)
    customer_info = get_customer_information(token)
    
    logger.info("Magento customer pipeline completed successfully")
    return customer_info


if __name__ == "__main__":
    magento_customer_pipeline()