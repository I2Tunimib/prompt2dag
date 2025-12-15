from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.exceptions import PrefectException
from datetime import timedelta
import requests

@task(retries=1, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def create_customer():
    """
    Executes a GraphQL mutation to create a new customer account.
    Returns the created customer's email address.
    """
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
            "firstName": "John",
            "lastName": "Doe",
            "email": "john.doe@example.com",
            "password": "securepassword123"
        }
    }
    response = requests.post(
        "https://magento.example.com/graphql",
        json={"query": mutation, "variables": variables}
    )
    if response.status_code != 200:
        raise PrefectException(f"Failed to create customer: {response.text}")
    data = response.json()
    return data["data"]["createCustomer"]["customer"]["email"]

@task(retries=1, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def generate_customer_token(email: str):
    """
    Uses the customer email to execute a GraphQL mutation that generates an authentication token.
    Returns the generated bearer token.
    """
    mutation = """
    mutation GenerateCustomerToken($email: String!, $password: String!) {
        generateCustomerToken(email: $email, password: $password) {
            token
        }
    }
    """
    variables = {
        "email": email,
        "password": "securepassword123"
    }
    response = requests.post(
        "https://magento.example.com/graphql",
        json={"query": mutation, "variables": variables}
    )
    if response.status_code != 200:
        raise PrefectException(f"Failed to generate customer token: {response.text}")
    data = response.json()
    return data["data"]["generateCustomerToken"]["token"]

@task(retries=1, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def get_customer_information(token: str):
    """
    Uses the authentication token to execute a GraphQL query that retrieves comprehensive customer profile data.
    """
    query = """
    query GetCustomerInformation {
        customer {
            id
            firstName
            lastName
            email
            addresses {
                id
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
        "Authorization": f"Bearer {token}"
    }
    response = requests.post(
        "https://magento.example.com/graphql",
        json={"query": query},
        headers=headers
    )
    if response.status_code != 200:
        raise PrefectException(f"Failed to get customer information: {response.text}")
    data = response.json()
    return data["data"]["customer"]

@flow(retries=1, retry_delay_seconds=300)
def customer_management_pipeline():
    """
    Orchestrates the customer management pipeline.
    """
    logger = get_run_logger()
    logger.info("Starting customer management pipeline")

    # Step 1: Create Customer
    email = create_customer()

    # Step 2: Generate Customer Token
    token = generate_customer_token(email)

    # Step 3: Get Customer Information
    customer_info = get_customer_information(token)

    logger.info(f"Customer information retrieved: {customer_info}")

if __name__ == '__main__':
    customer_management_pipeline()