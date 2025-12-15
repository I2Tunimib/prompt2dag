from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.exceptions import PrefectException
from datetime import timedelta
import requests

@task(retries=1, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def create_customer():
    logger = get_run_logger()
    url = "https://magento.example.com/graphql"
    headers = {"Content-Type": "application/json"}
    mutation = """
    mutation {
        createCustomer(input: { firstname: "John", lastname: "Doe", email: "john.doe@example.com", password: "securepassword123" }) {
            customer {
                email
            }
        }
    }
    """
    try:
        response = requests.post(url, json={"query": mutation}, headers=headers)
        response.raise_for_status()
        data = response.json()
        customer_email = data["data"]["createCustomer"]["customer"]["email"]
        logger.info(f"Customer created with email: {customer_email}")
        return customer_email
    except requests.RequestException as e:
        logger.error(f"Error creating customer: {e}")
        raise PrefectException("Failed to create customer") from e

@task(retries=1, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def generate_customer_token(customer_email):
    logger = get_run_logger()
    url = "https://magento.example.com/graphql"
    headers = {"Content-Type": "application/json"}
    mutation = f"""
    mutation {{
        generateCustomerToken(email: "{customer_email}", password: "securepassword123") {{
            token
        }}
    }}
    """
    try:
        response = requests.post(url, json={"query": mutation}, headers=headers)
        response.raise_for_status()
        data = response.json()
        token = data["data"]["generateCustomerToken"]["token"]
        logger.info(f"Generated token for customer: {customer_email}")
        return token
    except requests.RequestException as e:
        logger.error(f"Error generating token for customer: {customer_email}")
        raise PrefectException("Failed to generate token") from e

@task(retries=1, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def get_customer_information(token):
    logger = get_run_logger()
    url = "https://magento.example.com/graphql"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
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
    try:
        response = requests.post(url, json={"query": query}, headers=headers)
        response.raise_for_status()
        data = response.json()
        customer_info = data["data"]["customer"]
        logger.info(f"Retrieved customer information: {customer_info}")
        return customer_info
    except requests.RequestException as e:
        logger.error(f"Error retrieving customer information")
        raise PrefectException("Failed to retrieve customer information") from e

@flow
def customer_management_flow():
    logger = get_run_logger()
    logger.info("Starting customer management flow")
    customer_email = create_customer()
    token = generate_customer_token(customer_email)
    customer_info = get_customer_information(token)
    logger.info("Customer management flow completed successfully")

if __name__ == "__main__":
    customer_management_flow()