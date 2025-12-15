import logging
from datetime import timedelta

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

CREATE_CUSTOMER_MUTATION = """
mutation {
  createCustomer(
    input: {
      firstname: "John"
      lastname: "Doe"
      email: "john.doe@example.com"
      password: "SecurePassword123!"
    }
  ) {
    customer {
      email
    }
  }
}
"""

GENERATE_TOKEN_MUTATION = """
mutation generateCustomerToken($email: String!, $password: String!) {
  generateCustomerToken(email: $email, password: $password) {
    token
  }
}
"""

GET_CUSTOMER_QUERY = """
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
        region_code
      }
      postcode
      country_code
      telephone
    }
  }
}
"""


def create_customer(**context):
    """Create a new Magento customer account via GraphQL."""
    try:
        conn = BaseHook.get_connection('magento_default')
        graphql_url = f"{conn.host.rstrip('/')}/graphql"
        
        response = requests.post(
            graphql_url,
            json={'query': CREATE_CUSTOMER_MUTATION},
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            error_msg = f"GraphQL errors: {data['errors']}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        customer_email = data['data']['createCustomer']['customer']['email']
        logger.info(f"Successfully created customer: {customer_email}")
        
        context['ti'].xcom_push(key='customer_email', value=customer_email)
        return customer_email
        
    except Exception as e:
        logger.error(f"Failed to create customer: {str(e)}")
        raise


def generate_customer_token(**context):
    """Generate authentication token for the created customer."""
    try:
        ti = context['ti']
        customer_email = ti.xcom_pull(task_ids='create_customer', key='customer_email')
        
        if not customer_email:
            raise ValueError("Customer email not found")
        
        logger.info(f"Generating token for: {customer_email}")
        
        conn = BaseHook.get_connection('magento_default')
        graphql_url = f"{conn.host.rstrip('/')}/graphql"
        
        variables = {
            "email": customer_email,
            "password": "SecurePassword123!"
        }
        
        response = requests.post(
            graphql_url,
            json={'query': GENERATE_TOKEN_MUTATION, 'variables': variables},
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            error_msg = f"GraphQL errors: {data['errors']}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        token = data['data']['generateCustomerToken']['token']
        logger.info("Token generated successfully")
        
        ti.xcom_push(key='customer_token', value=token)
        return token
        
    except Exception as e:
        logger.error(f"Failed to generate token: {str(e)}")
        raise


def get_customer_information(**context):
    """Retrieve customer profile data using authentication token."""
    try:
        ti = context['ti']
        customer_token = ti.xcom_pull(task_ids='generate_customer_token', key='customer_token')
        
        if not customer_token:
            raise ValueError("Customer token not found")
        
        logger.info("Retrieving customer information")
        
        conn = BaseHook.get_connection('magento_default')
        graphql_url = f"{conn.host.rstrip('/')}/graphql"
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {customer_token}'
        }
        
        response = requests.post(
            graphql_url,
            json={'query': GET_CUSTOMER_QUERY},
            headers=headers,
            timeout=30
        )
        
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            error_msg = f"GraphQL errors: {data['errors']}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        customer_info = data['data']['customer']
        logger.info(f"Customer retrieved: {customer_info.get('email')}")
        
        return customer_info
        
    except Exception as e:
        logger.error(f"Failed to retrieve customer info: {str(e)}")
        raise


with DAG(
    dag_id='magento_customer_management',
    default_args=default_args,
    description='Sequential Magento customer management via GraphQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['magento', 'customer', 'graphql'],
) as dag:
    
    create_customer_task = PythonOperator(
        task_id='create_customer',
        python_callable=create_customer,
    )
    
    generate_customer_token_task = PythonOperator(
        task_id='generate_customer_token',
        python_callable=generate_customer_token,
    )
    
    get_customer_information_task = PythonOperator(
        task_id='get_customer_information',
        python_callable=get_customer_information,
    )
    
    create_customer_task >> generate_customer_token_task >> get_customer_information_task