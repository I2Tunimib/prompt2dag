<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements.

First, the pipeline has three sequential steps: Create Customer, Generate Customer Token, and Get Customer Information. Each step depends on the previous one's output. So, the tasks should be linear with explicit dependencies.

The DAG should have manual trigger only, so schedule_interval is None. Catchup needs to be disabled. Retry policy is one retry with a 5-minute delay. I'll set that in default_args along with other parameters like start_date and owner.

Since the tasks involve GraphQL operations, I'll use PythonOperators. Each task will be a Python function. The functions need to accept **kwargs to access XComs. For example, the create_customer function will push the email, which the generate_token function pulls, and so on.

The Magento connection is required, so I should reference 'magento_default' in the code. The functions should handle exceptions and log errors. I'll add try-except blocks in each function and use logging.

Data passing between tasks is done via XCom. So, each task will get the previous task's output using xcom_pull, specifying the task_ids.

Now, about imports. The user specified Airflow 2.x style, so I'll import DAG from airflow, and PythonOperator from airflow.operators.python. Also, since there's no branching or sensors mentioned, I don't need to include those modules.

Default_args should include the retry parameters, start_date (I'll set a default like '2023-01-01'), and other common settings. The DAG is named 'magento_customer_management' with the specified schedule and catchup.

Each task is a PythonOperator calling the respective function. Dependencies are set using the >> operator in the order create_customer_task >> generate_token_task >> get_customer_info_task.

I need to make sure the code is PEP 8 compliant, with proper indentation and spacing. Also, include docstrings for the functions and the DAG.

Wait, the functions need to interact with Magento's GraphQL API. How to handle that? The user mentioned using the magento_default connection, so I'll assume that the connection is set up with the necessary credentials. The functions can use requests library to make HTTP calls, using the connection details from Airflow's BaseHook.

So, in each function, I'll retrieve the magento_default connection, get the host (probably the API URL), and any authentication details. For the Generate Customer Token step, the authentication might be via the customer's email and password, which are part of the mutation. The Get Customer Information step uses the bearer token from the previous task.

Wait, the Create Customer step uses a GraphQL mutation. The code for that would involve sending a POST request to the Magento GraphQL endpoint with the mutation. Similarly for generating the token and fetching customer info.

I need to implement each of these steps. Let's outline each function:

1. create_customer(**kwargs):
   - Get Magento connection (host, maybe login/password or token?)
   - Build the GraphQL mutation for creating a customer.
   - Send the request.
   - Check response, handle errors.
   - Push customer email to XCom.

2. generate_token(**kwargs):
   - Pull the email from XCom (from create_customer_task)
   - Get Magento connection details.
   - Build the mutation for generating a token using the email and password.
   - Send request, get token.
   - Push token to XCom.

3. get_customer_info(**kwargs):
   - Pull the token from XCom (generate_token_task)
   - Get Magento connection details.
   - Build the query for customer info, include the bearer token in headers.
   - Send request, handle response.

But wait, the password is predefined. The description mentions "predefined personal details including name, email, and password". So the password is a fixed value. Should I hardcode it or retrieve it from a Variable? The user didn't specify, so maybe hardcode for simplicity, but note that in a real scenario it should be stored securely. For the example, I'll hardcode it but mention in a comment.

So in create_customer, the password is part of the mutation. Similarly, generate_token uses the same password.

But how to handle the Magento connection? The connection might have the base URL, and maybe an API key. For example, the host could be 'https://magento.example.com/graphql', and the connection might include extra fields like an API key or username/password. But the problem description says authentication for data retrieval is via the bearer token generated in the second step. So the first two steps might use a different authentication method, perhaps the Magento admin token.

Wait, creating a customer might require admin credentials, while generating a token uses the customer's email and password. So the magento_default connection might include an admin token. Let me think.

In the create_customer function, the GraphQL mutation would need an admin token in the headers. The connection's password field could store this token. So, when retrieving the connection, we can get the password as the bearer token for admin access.

Similarly, when generating the customer token, the mutation uses the customer's email and password (which is predefined). The admin token isn't needed here because it's a customer-facing mutation.

Wait, perhaps the Magento API requires an admin token to create a customer, and then the customer's own credentials to generate their token. So the create_customer task uses the admin token from the connection, while generate_token uses the customer's email and password (which are predefined, as per the description).

So, in code:

For create_customer:
- Get the magento_default connection.
- Host is the GraphQL endpoint.
- The admin token is in the connection's password field.
- Send the mutation with the admin token in the headers.

For generate_token:
- The mutation uses the customer's email (from XCom) and a hardcoded password.
- No admin token needed here, as this is a customer authentication step.

For get_customer_info:
- Use the customer's bearer token (from XCom) in the Authorization header.

So, putting this together:

In create_customer function:
- Get connection using BaseHook.get_connection('magento_default')
- host = conn.host
- admin_token = conn.password
- headers = {'Authorization': f'Bearer {admin_token}'}
- Send GraphQL mutation to create customer with email, password, etc.

In generate_token function:
- Get connection host
- No admin token here
- Send mutation with variables { email: email_from_xcom, password: hardcoded }
- Get the token from response

In get_customer_info function:
- Get connection host
- Use the token from XCom in headers: {'Authorization': f'Bearer {token}'}
- Send query to get customer info

But the password is hardcoded. The description says "predefined personal details", so perhaps the password is a default like 'Password123'. In the code, I'll hardcode it but note that in a real scenario, it should be retrieved securely.

Now, implementing the GraphQL requests. Using the requests library. Each function will construct the query/mutation, send a POST request, and handle the response.

Each function should have error handling. For example, if the response status code is not 200, raise an exception. Also, check if the response JSON contains errors.

So, in code:

In create_customer:

try:
    response = requests.post(url, json={'query': mutation, 'variables': variables}, headers=headers)
    response.raise_for_status()
    data = response.json()
    if 'errors' in data:
        logger.error(...)
        raise Exception(...)
    email = data['data']['createCustomer']['customer']['email']
    kwargs['ti'].xcom_push(key='customer_email', value=email)
except ...:

Similarly for the other functions.

Now, putting all this together into the code.

Wait, the user mentioned that the pipeline returns the email, token, etc. So each task pushes its output to XCom, which the next task pulls.

So, in create_customer_task, the function pushes 'customer_email' to XCom. Then generate_token_task pulls it via ti.xcom_pull(task_ids='create_customer_task', key='customer_email').

Same for the token being passed to get_customer_info_task.

Now, writing the code.

Imports needed: datetime for DAG, PythonOperator, DAG, BaseHook from airflow.hooks.base, requests, logging, maybe json for formatting.

Wait, the user said to use Airflow 2.x style imports. So:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import requests
from datetime import datetime, timedelta
import logging

Then, default_args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

The DAG definition:

with DAG(
    'magento_customer_management',
    default_args=default_args,
    description='Magento customer management pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['magento'],
) as dag:

Then the three PythonOperator tasks.

Implementing the create_customer function:

def create_customer(**kwargs):
    logger = logging.getLogger(__name__)
    try:
        conn = BaseHook.get_connection('magento_default')
        url = conn.host
        admin_token = conn.password
        headers = {'Authorization': f'Bearer {admin_token}'}
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
            'input': {
                'firstname': 'John',
                'lastname': 'Doe',
                'email': 'john.doe@example.com',
                'password': 'Password123'
            }
        }
        response = requests.post(url, json={'query': mutation, 'variables': variables}, headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            error_msg = data['errors'][0]['message']
            logger.error(f"GraphQL error: {error_msg}")
            raise Exception(f"Customer creation failed: {error_msg}")
        email = data['data']['createCustomer']['customer']['email']
        logger.info(f"Created customer with email: {email}")
        kwargs['ti'].xcom_push(key='customer_email', value=email)
    except Exception as e:
        logger.error(f"Error creating customer: {str(e)}")
        raise

Similarly for generate_token:

def generate_token(**kwargs):
    logger = logging.getLogger(__name__)
    ti = kwargs['ti']
    customer_email = ti.xcom_pull(task_ids='create_customer_task', key='customer_email')
    try:
        conn = BaseHook.get_connection('magento_default')
        url = conn.host
        mutation = """
        mutation GenerateCustomerToken($email: String!, $password: String!) {
            generateCustomerToken(email: $email, password: $password) {
                token
            }
        }
        """
        variables = {
            'email': customer_email,
            'password': 'Password123'  # Same as in createCustomer
        }
        response = requests.post(url, json={'query': mutation, 'variables': variables})
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            error_msg = data['errors'][0]['message']
            logger.error(f"GraphQL error: {error_msg}")
            raise Exception(f"Token generation failed: {error_msg}")
        token = data['data']['generateCustomerToken']['token']
        logger.info("Successfully generated customer token")
        ti.xcom_push(key='auth_token', value=token)
    except Exception as e:
        logger.error(f"Error generating token: {str(e)}")
        raise

And get_customer_info:

def get_customer_info(**kwargs):
    logger = logging.getLogger(__name__)
    ti = kwargs['ti']
    auth_token = ti.xcom_pull(task_ids='generate_token_task', key='auth_token')
    try:
        conn = BaseHook.get_connection('magento_default')
        url = conn.host
        query = """
        query GetCustomer {
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
        headers = {'Authorization': f'Bearer {auth_token}'}
        response = requests.post(url, json={'query': query}, headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            error_msg = data['errors'][0]['message']
            logger.error(f"GraphQL error: {error_msg}")
            raise Exception(f"Customer info retrieval failed: {error_msg}")
        customer_data = data['data']['customer']
        logger.info(f"Retrieved customer data: {customer_data}")
        ti.xcom_push(key='customer_data', value=customer_data)
    except Exception as e:
        logger.error(f"Error retrieving customer info: {str(e)}")
        raise

Then, define the tasks:

create_customer_task = PythonOperator(
    task_id='create_customer_task',
    python_callable=create_customer,
)

generate_token_task = PythonOperator(
    task_id='generate_token_task',
    python_callable=generate_token,
)

get_customer_info_task = PythonOperator(
    task_id='get_customer_info_task',
    python_callable=get_customer_info,
)

Set dependencies:

create_customer_task >> generate_token_task >> get_customer_info_task

Putting all together, ensuring proper indentation and imports.

Wait, the Magento connection's host should be the GraphQL endpoint, like 'https://magento.example.com/graphql'. The admin token is stored in the connection's password field.

In the code, the create_customer function uses the admin token from the connection's password. The generate_token function doesn't use any connection details except the host URL, as it's using the customer's credentials.

The password in the create_customer variables is hardcoded as 'Password123', which matches the predefined details. Similarly, the generate_token variables use the same password.

This should cover the requirements. The code is PEP 8 compliant, uses Airflow 2.x imports, has proper task dependencies, handles errors, and passes data via XCom.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_customer(**kwargs):
    """Create new customer account via Magento GraphQL API"""
    logger = logging.getLogger(__name__)
    try:
        conn = BaseHook.get_connection('magento_default')
        url = conn.host
        admin_token = conn.password
        
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
                "firstname": "John",
                "lastname": "Doe",
                "email": "john.doe@example.com",
                "password": "Password123"
            }
        }

        headers = {"Authorization": f"Bearer {admin_token}"}
        response = requests.post(url, json={'query': mutation, 'variables': variables}, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        if 'errors' in data:
            error_msg = data['errors'][0]['message']
            logger.error(f"GraphQL error: {error_msg}")
            raise Exception(f"Customer creation failed: {error_msg}")

        email = data['data']['createCustomer']['customer']['email']
        logger.info(f"Created customer with email: {email}")
        kwargs['ti'].xcom_push(key='customer_email', value=email)
    except Exception as e:
        logger.error(f"Error creating customer: {str(e)}")
        raise

def generate_token(**kwargs):
    """Generate authentication token for created customer"""
    logger = logging.getLogger(__name__)
    ti = kwargs['ti']
    customer_email = ti.xcom_pull(task_ids='create_customer_task', key='customer_email')
    
    try:
        conn = BaseHook.get_connection('magento_default')
        url = conn.host
        
        mutation = """
        mutation GenerateCustomerToken($email: String!, $password: String!) {
            generateCustomerToken(email: $email, password: $password) {
                token
            }
        }
        """
        
        variables = {
            "email": customer_email,
            "password": "Password123"
        }

        response = requests.post(url, json={'query': mutation, 'variables': variables})
        response.raise_for_status()
        
        data = response.json()
        if 'errors' in data:
            error_msg = data['errors'][0]['message']
            logger.error(f"GraphQL error: {error_msg}")
            raise Exception(f"Token generation failed: {error_msg}")

        token = data['data']['generateCustomerToken']['token']
        logger.info("Successfully generated customer token")
        ti.xcom_push(key='auth_token', value=token)
    except Exception as e:
        logger.error(f"Error generating token: {str(e)}")
        raise

def get_customer_info(**kwargs):
    """Retrieve customer profile using authentication token"""
    logger = logging.getLogger(__name__)
    ti = kwargs['ti']
    auth_token = ti.xcom_pull(task_ids='generate_token_task', key='auth_token')
    
    try:
        conn = BaseHook.get_connection('magento_default')
        url = conn.host
        
        query = """
        query GetCustomer {
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

        headers = {"Authorization": f"Bearer {auth_token}"}
        response = requests.post(url, json={'query': query}, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        if 'errors' in data:
            error_msg = data['errors'][0]['message']
            logger.error(f"GraphQL error: {error_msg}")
            raise Exception(f"Customer info retrieval failed: {error_msg}")

        customer_data = data['data']['customer']
        logger.info(f"Retrieved customer data: {customer_data}")
        ti.xcom_push(key='customer_data', value=customer_data)
    except Exception as e:
        logger.error(f"Error retrieving customer info: {str(e)}")
        raise

with DAG(
    'magento_customer_management',
    default_args=default_args,
    description='Magento customer management pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['magento'],
) as dag:

    create_customer_task = PythonOperator(
        task_id='create_customer_task',
        python_callable=create_customer,
    )

    generate_token_task = PythonOperator(
        task_id='generate_token_task',
        python_callable=generate_token,
    )

    get_customer_info_task = PythonOperator(
        task_id='get_customer_info_task',
        python_callable=get_customer_info,
    )

    create_customer_task >> generate_token_task >> get_customer_info_task