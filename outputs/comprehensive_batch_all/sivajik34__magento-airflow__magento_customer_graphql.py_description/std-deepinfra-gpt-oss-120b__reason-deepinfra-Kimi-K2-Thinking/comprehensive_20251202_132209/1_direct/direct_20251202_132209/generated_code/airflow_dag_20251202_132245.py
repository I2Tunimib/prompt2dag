from datetime import timedelta

import logging
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def _get_magento_endpoint():
    """
    Retrieve Magento GraphQL endpoint from the ``magento_default`` connection.
    The connection's ``host`` field should contain the base URL (e.g., https://example.com).
    """
    conn = BaseHook.get_connection("magento_default")
    base_url = conn.host.rstrip("/")
    return f"{base_url}/graphql"


def create_customer(**kwargs):
    """
    Create a new Magento customer via GraphQL mutation.
    Returns the customer's email address for downstream tasks.
    """
    endpoint = _get_magento_endpoint()
    # Example payload – replace with real values or make them configurable.
    mutation = """
    mutation CreateCustomer($email: String!, $password: String!, $firstname: String!, $lastname: String!) {
      createCustomer(
        input: {
          email: $email
          password: $password
          firstname: $firstname
          lastname: $lastname
        }
      ) {
        customer {
          email
        }
      }
    }
    """
    variables = {
        "email": "new.customer@example.com",
        "password": "SecurePass123!",
        "firstname": "New",
        "lastname": "Customer",
    }

    try:
        response = requests.post(
            endpoint,
            json={"query": mutation, "variables": variables},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        email = data["data"]["createCustomer"]["customer"]["email"]
        logging.info("Customer created with email: %s", email)
        return email
    except Exception as exc:
        logging.error("Failed to create customer: %s", exc)
        raise


def generate_customer_token(**kwargs):
    """
    Generate an authentication token for the customer created in the previous step.
    Returns the bearer token for downstream tasks.
    """
    ti = kwargs["ti"]
    email = ti.xcom_pull(task_ids="create_customer")
    if not email:
        raise ValueError("Customer email not found in XCom.")

    endpoint = _get_magento_endpoint()
    mutation = """
    mutation GenerateToken($email: String!, $password: String!) {
      generateCustomerToken(email: $email, password: $password) {
        token
      }
    }
    """
    variables = {
        "email": email,
        "password": "SecurePass123!",  # Must match password used in creation
    }

    try:
        response = requests.post(
            endpoint,
            json={"query": mutation, "variables": variables},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        token = data["data"]["generateCustomerToken"]["token"]
        logging.info("Generated token for %s", email)
        return token
    except Exception as exc:
        logging.error("Failed to generate token for %s: %s", email, exc)
        raise


def get_customer_information(**kwargs):
    """
    Retrieve detailed customer information using the token generated earlier.
    """
    ti = kwargs["ti"]
    token = ti.xcom_pull(task_ids="generate_customer_token")
    if not token:
        raise ValueError("Customer token not found in XCom.")

    endpoint = _get_magento_endpoint()
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
        response = requests.post(
            endpoint,
            json={"query": query},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        customer_info = data["data"]["customer"]
        logging.info("Retrieved customer information: %s", customer_info)
        # Optionally push the info to XCom for downstream consumption
        return customer_info
    except Exception as exc:
        logging.error("Failed to retrieve customer information: %s", exc)
        raise


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

with DAG(
    dag_id="magento_customer_onboarding",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["magento", "customer", "graphql"],
) as dag:
    create_customer_task = PythonOperator(
        task_id="create_customer",
        python_callable=create_customer,
        provide_context=True,
    )

    generate_token_task = PythonOperator(
        task_id="generate_customer_token",
        python_callable=generate_customer_token,
        provide_context=True,
    )

    get_info_task = PythonOperator(
        task_id="get_customer_information",
        python_callable=get_customer_information,
        provide_context=True,
    )

    create_customer_task >> generate_token_task >> get_info_task