<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements.

First, the pipeline has three sequential steps: Create Customer, Generate Customer Token, and Get Customer Information. Each step depends on the previous one's output. So the tasks should be linear, each depending on the prior.

The DAG should have manual triggers only, so schedule_interval is None. Catchup needs to be disabled. Retry policy is one retry with a 5-minute delay. Default_args should include these retries, along with other standard parameters like start_date and maybe owner.

The tasks will use PythonOperators since they're executing Python functions. Each function will handle the GraphQL operations. Data passing between tasks is done via XComs. So, the first task pushes the customer email, the second uses that email to get the token, then the third uses the token to retrieve info.

The Magento connection is required, using 'magento_default' connection ID. Each task needs to retrieve this connection. I'll assume the connection is set up with the necessary details like base URL and credentials.

For error handling, each function should have try-except blocks, logging exceptions and raising Airflow exceptions to trigger retries.

Now, structuring the code:

1. Import necessary modules: DAG, PythonOperator, and maybe others like exceptions for Airflow.
2. Define default_args with retries, retry_delay, etc.
3. Instantiate the DAG with the specified parameters.
4. Define the three functions for each task, making sure they accept **context to access XComs.
5. In each function, pull the required data from the previous task using XComs. For example, the Generate Customer Token function needs the email from Create Customer.
6. Use the Magento connection details. How to get that? Using BaseHook to get the connection and then construct the API URL.
7. Implement the GraphQL requests. Using requests library for HTTP calls. Handle possible HTTP errors and exceptions.
8. Push the required data (email, token) to XCom so the next task can use it.
9. Set up the task dependencies with the >> operator to make them sequential.

Wait, the user mentioned data passing between tasks. So each task pushes its output, and the next task pulls it via XCom. The functions need to have provide_context=True in the PythonOperator to access the context.

Also, need to make sure that the start_date is set properly. Since it's a manual trigger, maybe set a fixed date in the past, like '2023-01-01'.

Let me outline the code structure:

- Imports: DAG, PythonOperator, exceptions, hooks, maybe datetime for start_date.
- Default args: retries=1, retry_delay=5 minutes, other standard params.
- DAG definition: dag_id, schedule_interval=None, catchup=False, etc.
- Define the three functions: create_customer, generate_customer_token, get_customer_info.
- Each function uses BaseHook.get_connection('magento_default') to get host, maybe port, login (which could be the API key?), etc.
- Construct the GraphQL endpoint URL from the connection details.
- For create_customer: send a mutation to create the customer, return email.
- For generate_customer_token: use the email from XCom to get the token.
- For get_customer_info: use the token from XCom in the headers as Bearer token.
- Each task is a PythonOperator calling the respective function, with provide_context=True.
- Set dependencies: create >> generate >> get.

Potential issues: How to handle the Magento connection details. The connection should have the host (like base URL), maybe an API key in the password field. The functions will need to extract these details.

Testing for exceptions: Each function should catch exceptions, log them, and raise AirflowFailException or similar to trigger retries.

Also, the functions need to return values so that XCom can store them. For example, create_customer returns the email, which is then pulled by generate_customer_token.

Wait, in Airflow, when a PythonOperator's python_callable returns a value, it's automatically pushed to XCom. So each function can return the necessary data (email, token), and the next task can pull it via context['ti'].xcom_pull(task_ids='previous_task').

So in the create_customer function, after creating, return the email. The generate_customer_token function will pull this email from the previous task's XCom.

Similarly, generate_customer_token returns the token, which get_customer_info pulls.

Now, writing the code:

Start with imports:

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

Then default_args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

Wait, need to import timedelta. So add:

from datetime import timedelta

Then the DAG:

with DAG(
    dag_id='magento_customer_management',
    default_args=default_args,
    description='Manages Magento customer lifecycle via GraphQL',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['magento'],
) as dag:

Then define the functions inside the DAG context or outside? Probably outside, but in the code structure, they can be defined before the DAG block.

Wait, in Python, functions can be defined outside the DAG's with block. So:

def create_customer(**context):
    # code here

def generate_customer_token(**context):
    # code here

def get_customer_info(**context):
    # code here

Then, inside the DAG block, instantiate the tasks:

create_customer_task = PythonOperator(
    task_id='create_customer',
    python_callable=create_customer,
    provide_context=True,
)

generate_token_task = PythonOperator(
    task_id='generate_customer_token',
    python_callable=generate_customer_token,
    provide_context=True,
)

get_customer_info_task = PythonOperator(
    task_id='get_customer_information',
    python_callable=get_customer_info,
    provide_context=True,
)

Then dependencies:

create_customer_task >> generate_token_task >> get_customer_info_task

Now, filling in the functions.

For create_customer:

def create_customer(**context):
    from airflow.hooks.base import BaseHook
    import requests
    import json

    # Get Magento connection
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host
    # Assuming API key is in password or extra
    api_key = conn.password

    # GraphQL mutation
    mutation = """
    mutation {
        createCustomer(input: {
            firstname: "John",
            lastname: "Doe",
            email: "john.doe@example.com",
            password: "securePassword123"
        }) {
            customer {
                email
            }
        }
    }
    """
    url = f"{base_url}/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    payload = {'query': mutation}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            raise AirflowException(f"GraphQL error: {data['errors']}")
        email = data['data']['createCustomer']['customer']['email']
        return email
    except requests.exceptions.RequestException as e:
        context['ti'].log.error(f"Request failed: {e}")
        raise AirflowException(e)
    except KeyError as e:
        context['ti'].log.error(f"Missing key in response: {e}")
        raise AirflowException(e)

Wait, but the email is returned, which is pushed to XCom. Then the next task can pull it.

Similarly, generate_customer_token:

def generate_customer_token(**context):
    from airflow.hooks.base import BaseHook
    import requests
    import json

    # Get previous task's email
    ti = context['ti']
    email = ti.xcom_pull(task_ids='create_customer')

    # Get Magento connection
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host
    api_key = conn.password

    mutation = f"""
    mutation {{
        generateCustomerToken(email: "{email}", password: "securePassword123") {{
            token
        }}
    }}
    """
    url = f"{base_url}/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    payload = {'query': mutation}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            raise AirflowException(f"GraphQL error: {data['errors']}")
        token = data['data']['generateCustomerToken']['token']
        return token
    except requests.exceptions.RequestException as e:
        context['ti'].log.error(f"Request failed: {e}")
        raise AirflowException(e)
    except KeyError as e:
        context['ti'].log.error(f"Missing key in response: {e}")
        raise AirflowException(e)

Wait, but the password here is hardcoded. The pipeline description says the password is predefined. So maybe the password is part of the initial customer creation step. But in the description, the Generate Customer Token step uses the same password. So perhaps the password is fixed, but in reality, it's better to pass it via a Variable or the connection. However, the description says "predefined personal details including name, email, and password". So maybe the password is hardcoded in the mutation. But for security, it's better not to hardcode. However, the user hasn't specified any Variables, so perhaps it's acceptable here as per the problem statement.

Similarly, the Get Customer Information function:

def get_customer_info(**context):
    from airflow.hooks.base import BaseHook
    import requests
    import json

    # Get token from previous task
    ti = context['ti']
    token = ti.xcom_pull(task_ids='generate_customer_token')

    # Get Magento connection
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host

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
                postcode
                country_code
            }
        }
    }
    """
    url = f"{base_url}/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    payload = {'query': query}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            raise AirflowException(f"GraphQL error: {data['errors']}")
        customer_data = data['data']['customer']
        context['ti'].log.info(f"Customer data retrieved: {customer_data}")
        return customer_data
    except requests.exceptions.RequestException as e:
        context['ti'].log.error(f"Request failed: {e}")
        raise AirflowException(e)
    except KeyError as e:
        context['ti'].log.error(f"Missing key in response: {e}")
        raise AirflowException(e)

Wait, but in the Get Customer Information task, the Authorization header uses the token generated earlier. So the token is passed via XCom from the generate task.

Putting it all together, the code should look like this. Also, ensure that all necessary imports are included. For example, the 'requests' library needs to be imported inside each function if it's not installed globally, but in Airflow, it's common to have it as a dependency. However, since the user said to assume required packages are installed, it's okay.

But wait, in the code above, the functions import requests inside them. That's because if the DAG file is parsed without requests installed, it would cause a top-level import error. By importing inside the function, it's only imported when the task runs, which allows the DAG to be parsed even if requests isn't installed, but the task would fail at runtime. However, the user's constraints say to assume required packages are installed, so maybe it's better to import requests at the top. But the code as written imports inside the functions. Alternatively, move the imports to the top.

But the initial code skeleton didn't include 'import requests' at the top. So perhaps the user expects that the code includes all necessary imports. Let me adjust that.

So, adding 'import requests' at the top, along with 'import json' perhaps.

Wait, but the functions are defined outside the DAG's context. So the imports can be at the top.

So revised code:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import requests
import json

def create_customer(**context):
    # Get Magento connection
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host
    api_key = conn.password

    # GraphQL mutation
    mutation = """
    mutation {
        createCustomer(input: {
            firstname: "John",
            lastname: "Doe",
            email: "john.doe@example.com",
            password: "securePassword123"
        }) {
            customer {
                email
            }
        }
    }
    """
    url = f"{base_url}/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    payload = {'query': mutation}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            raise AirflowException(f"GraphQL error: {data['errors']}")
        email = data['data']['createCustomer']['customer']['email']
        return email
    except requests.exceptions.RequestException as e:
        context['ti'].log.error(f"Request failed: {e}")
        raise AirflowException(e)
    except KeyError as e:
        context['ti'].log.error(f"Missing key in response: {e}")
        raise AirflowException(e)

def generate_customer_token(**context):
    # Get previous task's email
    ti = context['ti']
    email = ti.xcom_pull(task_ids='create_customer')

    # Get Magento connection
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host
    api_key = conn.password

    mutation = f"""
    mutation {{
        generateCustomerToken(email: "{email}", password: "securePassword123") {{
            token
        }}
    }}
    """
    url = f"{base_url}/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    payload = {'query': mutation}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            raise AirflowException(f"GraphQL error: {data['errors']}")
        token = data['data']['generateCustomerToken']['token']
        return token
    except requests.exceptions.RequestException as e:
        context['ti'].log.error(f"Request failed: {e}")
        raise AirflowException(e)
    except KeyError as e:
        context['ti'].log.error(f"Missing key in response: {e}")
        raise AirflowException(e)

def get_customer_info(**context):
    # Get token from previous task
    ti = context['ti']
    token = ti.xcom_pull(task_ids='generate_customer_token')

    # Get Magento connection
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host

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
                postcode
                country_code
            }
        }
    }
    """
    url = f"{base_url}/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    payload = {'query': query}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            raise AirflowException(f"GraphQL error: {data['errors']}")
        customer_data = data['data']['customer']
        context['ti'].log.info(f"Customer data retrieved: {customer_data}")
        return customer_data
    except requests.exceptions.RequestException as e:
        context['ti'].log.error(f"Request failed: {e}")
        raise AirflowException(e)
    except KeyError as e:
        context['ti'].log.error(f"Missing key in response: {e}")
        raise AirflowException(e)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='magento_customer_management',
    default_args=default_args,
    description='Manages Magento customer lifecycle via GraphQL',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['magento'],
) as dag:

    create_customer_task = PythonOperator(
        task_id='create_customer',
        python_callable=create_customer,
    )

    generate_token_task = PythonOperator(
        task_id='generate_customer_token',
        python_callable=generate_customer_token,
    )

    get_customer_info_task = PythonOperator(
        task_id='get_customer_information',
        python_callable=get_customer_info,
    )

    create_customer_task >> generate_token_task >> get_customer_info_task

Wait, but in the PythonOperators, do we need to set provide_context=True? Because the functions are using the context. For PythonOperator, if you want to pass the context (like ti), you need to set provide_context=True. Otherwise, the **context argument won't be populated.

So, in each PythonOperator, add provide_context=True.

So:

create_customer_task = PythonOperator(
    task_id='create_customer',
    python_callable=create_customer,
    provide_context=True,
)

Same for the others.

Otherwise, the context parameter in the functions would be empty. So that's a crucial point. Without provide_context=True, the functions won't have access to the task instance (ti) and thus can't pull XCom values.

So adding provide_context=True to all three tasks.

Now, testing for possible errors. For example, in the generate_customer_token function, the email is pulled from XCom. If the previous task failed to push it, this would raise an error. But since the tasks are sequential, if create_customer fails, generate won't run.

Also, the Magento connection's 'magento_default' is expected to exist. The code doesn't handle if the connection is missing, but that's beyond the scope here.

The code uses hardcoded values for firstname, lastname, email, and password in the create mutation. According to the pipeline description, these are predefined, so it's acceptable. In a real-world scenario, these would probably be parameters or variables, but the problem statement doesn't mention that, so we proceed as per the description.

Finally, the DAG is structured with the three tasks in sequence. The code should be PEP 8 compliant, with proper indentation and imports.

I think that covers all the requirements. Now, putting it all together into the final code.
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import requests
import json

def create_customer(**context):
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host
    api_key = conn.password

    mutation = """
    mutation {
        createCustomer(input: {
            firstname: "John",
            lastname: "Doe",
            email: "john.doe@example.com",
            password: "securePassword123"
        }) {
            customer {
                email
            }
        }
    }
    """
    url = f"{base_url}/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }

    try:
        response = requests.post(url, headers=headers, json={'query': mutation})
        response.raise_for_status()
        data = response.json()
        if data.get('errors'):
            raise AirflowException(f"GraphQL error: {data['errors']}")
        return data['data']['createCustomer']['customer']['email']
    except Exception as e:
        context['ti'].log.error(f"Customer creation failed: {str(e)}")
        raise

def generate_customer_token(**context):
    ti = context['ti']
    email = ti.xcom_pull(task_ids='create_customer')
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host
    api_key = conn.password

    mutation = f"""
    mutation {{
        generateCustomerToken(email: "{email}", password: "securePassword123") {{
            token
        }}
    }}
    """
    url = f"{base_url}/graphql"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {api_key}'}

    try:
        response = requests.post(url, headers=headers, json={'query': mutation})
        response.raise_for_status()
        data = response.json()
        if data.get('errors'):
            raise AirflowException(f"GraphQL error: {data['errors']}")
        return data['data']['generateCustomerToken']['token']
    except Exception as e:
        context['ti'].log.error(f"Token generation failed: {str(e)}")
        raise

def get_customer_info(**context):
    ti = context['ti']
    token = ti.xcom_pull(task_ids='generate_customer_token')
    conn = BaseHook.get_connection('magento_default')
    base_url = conn.host

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
                postcode
                country_code
            }
        }
    }
    """
    url = f"{base_url}/graphql"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'}

    try:
        response = requests.post(url, headers=headers, json={'query': query})
        response.raise_for_status()
        data = response.json()
        if data.get('errors'):
            raise AirflowException(f"GraphQL error: {data['errors']}")
        context['ti'].log.info(f"Customer data: {json.dumps(data['data'], indent=2)}")
        return data['data']
    except Exception as e:
        context['ti'].log.error(f"Data retrieval failed: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1)
}

with DAG(
    dag_id='magento_customer_management',
    default_args=default_args,
    description='Magento customer onboarding pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['magento'],
) as dag:

    create_task = PythonOperator(
        task_id='create_customer',
        python_callable=create_customer,
        provide_context=True
    )

    generate_token_task = PythonOperator(
        task_id='generate_customer_token',
        python_callable=generate_customer_token,
        provide_context=True
    )

    get_info_task = PythonOperator(
        task_id='get_customer_information',
        python_callable=get_customer_info,
        provide_context=True
    )

    create_task >> generate_token_task >> get_info_task