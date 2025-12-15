from datetime import timedelta
import logging

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def create_customer(**context):
    """Create a new Magento customer via GraphQL mutation."""
    try:
        conn = BaseHook.get_connection("magento_default")
        endpoint = conn.extra_dejson.get("endpoint") or conn.host

        mutation = """
        mutation createCustomer($input: CustomerCreateInput!) {
          createCustomer(input: $input) {
            customer {
              email
            }
          }
        }
        """
        variables = {
            "input": {
                "email": "newcustomer@example.com",
                "firstname": "John",
                "lastname": "Doe",
                "password": "SecurePass123",
            }
        }

        response = requests.post(
            endpoint, json={"query": mutation, "variables": variables}
        )
        response.raise_for_status()
        data = response.json()
        email = data["data"]["createCustomer"]["customer"]["email"]
        logging.info("Created customer with email %s", email)
        return email
    except Exception as exc:
        logging.error("Failed to create customer: %s", exc)
        raise


def generate_customer_token(**context):
    """Generate an authentication token for the created customer."""
    try:
        email = context["ti"].xcom_pull(task_ids="create_customer")
        if not email:
            raise ValueError("Customer email not found in XCom.")

        conn = BaseHook.get_connection("magento_default")
        endpoint = conn.extra_dejson.get("endpoint") or conn.host

        mutation = """
        mutation generateToken($email: String!, $password: String!) {
          generateCustomerToken(email: $email, password: $password) {
            token
          }
        }
        """
        variables = {"email": email, "password": "SecurePass123"}

        response = requests.post(
            endpoint, json={"query": mutation, "variables": variables}
        )
        response.raise_for_status()
        data = response.json()
        token = data["data"]["generateCustomerToken"]["token"]
        logging.info("Generated token for %s", email)
        return token
    except Exception as exc:
        logging.error("Failed to generate token: %s", exc)
        raise


def get_customer_info(**context):
    """Retrieve the customer's profile information using the token."""
    try:
        token = context["ti"].xcom_pull(task_ids="generate_customer_token")
        if not token:
            raise ValueError("Authentication token not found in XCom.")

        conn = BaseHook.get_connection("magento_default")
        endpoint = conn.extra_dejson.get("endpoint") or conn.host

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

        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(
            endpoint, json={"query": query}, headers=headers
        )
        response.raise_for_status()
        data = response.json()
        logging.info("Retrieved customer information: %s", data)
        return data
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
    max_active_runs=1,
    tags=["magento"],
) as dag:
    create_customer_task = PythonOperator(
        task_id="create_customer",
        python_callable=create_customer,
    )

    generate_token_task = PythonOperator(
        task_id="generate_customer_token",
        python_callable=generate_customer_token,
    )

    get_customer_info_task = PythonOperator(
        task_id="get_customer_info",
        python_callable=get_customer_info,
    )

    create_customer_task >> generate_token_task >> get_customer_info_task