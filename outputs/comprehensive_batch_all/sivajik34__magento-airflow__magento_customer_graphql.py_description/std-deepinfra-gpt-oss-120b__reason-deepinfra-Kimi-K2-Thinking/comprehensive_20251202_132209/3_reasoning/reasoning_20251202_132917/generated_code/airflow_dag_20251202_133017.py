from datetime import timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
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
    }
  }
}
"""


def create_customer(**context):
    """Create a new Magento customer account via GraphQL mutation."""
    try:
        hook = HttpHook(method='POST', http_conn_id='magento_default')
        response = hook.run(
            endpoint='/graphql',
            json={'query': CREATE_CUSTOMER_MUTATION},
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
        data = response.json()
        
        customer_email = data['data']['createCustomer']['customer']['email']
        logger.info(f"Customer created successfully: {customer_email}")
        
        context['task_instance'].xcom_push(key='customer_email', value=customer_email)
        return customer_email
    except Exception as e:
        logger.error(f"Failed to create customer: {e}")
        raise


def generate_customer_token(**context):
    """Generate authentication token for the created customer."""
    try:
        ti = context['task_instance']
        customer_email = ti.xcom_pull(task_ids='create_customer', key='customer_email')
        
        if not customer_email:
            raise ValueError("Customer email not found from create_customer task")
        
        # Password should be stored securely; hardcoded here for demonstration
        password = "SecurePassword123!"
        
        hook = HttpHook(method='POST', http_conn_id='magento_default')
        response = hook.run(
            endpoint='/graphql',
            json={
                'query': GENERATE_TOKEN_MUTATION,
                'variables': {'email': customer_email, 'password': password}
            },
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
        data = response.json()
        
        token = data['data']['generateCustomerToken']['token']
        logger.info(f"Token generated successfully for customer: {customer_email}")
        
        ti.xcom_push(key='auth_token', value=token)
        return token
    except Exception as e:
        logger.error(f"Failed to generate customer token: {e}")
        raise


def get_customer_information(**context):
    """Retrieve customer profile information using authentication token."""
    try:
        ti = context['task_instance']
        auth_token = ti.xcom_pull(task_ids='generate_customer_token', key='auth_token')
        
        if not auth_token:
            raise ValueError("Auth token not found from generate_customer_token task")
        
        hook = HttpHook(method='POST', http_conn_id='magento_default')
        response = hook.run(
            endpoint='/graphql',
            json={'query': GET_CUSTOMER_QUERY},
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {auth_token}'
            }
        )
        response.raise_for_status()
        data = response.json()
        
        customer_info = data['data']['customer']
        logger.info(f"Customer information retrieved successfully: {customer_info}")
        
        return customer_info
    except Exception as e:
        logger.error(f"Failed to retrieve customer information: {e}")
        raise


with DAG(
    dag_id='magento_customer_management',
    default_args=default_args,
    description='Sequential Magento customer management operations using GraphQL',
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