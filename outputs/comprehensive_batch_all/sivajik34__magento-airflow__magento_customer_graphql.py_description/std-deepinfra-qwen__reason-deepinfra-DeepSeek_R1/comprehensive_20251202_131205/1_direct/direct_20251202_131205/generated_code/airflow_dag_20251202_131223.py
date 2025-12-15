from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.http_hook import HttpHook
import logging

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id='magento_customer_management',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['magento', 'customer', 'graphql']
) as dag:

    def create_customer(**kwargs):
        """Create a new customer account using GraphQL mutation."""
        http_hook = HttpHook(method='POST', http_conn_id='magento_default')
        mutation = """
        mutation {
            createCustomer(input: {
                email: "new.customer@example.com",
                firstname: "New",
                lastname: "Customer",
                password: "securepassword123"
            }) {
                customer {
                    email
                }
            }
        }
        """
        response = http_hook.run(endpoint='/graphql', json={'query': mutation})
        customer_email = response.json()['data']['createCustomer']['customer']['email']
        logging.info(f"Customer created with email: {customer_email}")
        return customer_email

    def generate_customer_token(**kwargs):
        """Generate an authentication token for the customer using GraphQL mutation."""
        ti = kwargs['ti']
        customer_email = ti.xcom_pull(task_ids='create_customer')
        http_hook = HttpHook(method='POST', http_conn_id='magento_default')
        mutation = f"""
        mutation {{
            generateCustomerToken(email: "{customer_email}", password: "securepassword123") {{
                token
            }}
        }}
        """
        response = http_hook.run(endpoint='/graphql', json={'query': mutation})
        token = response.json()['data']['generateCustomerToken']['token']
        logging.info(f"Generated token for customer: {token}")
        return token

    def get_customer_information(**kwargs):
        """Retrieve customer information using the generated authentication token."""
        ti = kwargs['ti']
        token = ti.xcom_pull(task_ids='generate_customer_token')
        http_hook = HttpHook(method='POST', http_conn_id='magento_default')
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
                    region
                    postcode
                    country_code
                }
            }
        }
        """
        headers = {'Authorization': f'Bearer {token}'}
        response = http_hook.run(endpoint='/graphql', json={'query': query}, headers=headers)
        customer_info = response.json()['data']['customer']
        logging.info(f"Customer information retrieved: {customer_info}")
        return customer_info

    # Define the tasks
    create_customer_task = PythonOperator(
        task_id='create_customer',
        python_callable=create_customer,
        provide_context=True
    )

    generate_customer_token_task = PythonOperator(
        task_id='generate_customer_token',
        python_callable=generate_customer_token,
        provide_context=True
    )

    get_customer_information_task = PythonOperator(
        task_id='get_customer_information',
        python_callable=get_customer_information,
        provide_context=True
    )

    # Define the task dependencies
    create_customer_task >> generate_customer_token_task >> get_customer_information_task