<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements.

First, the pipeline has four tasks in sequence: get_user, process_user, create_table, insert_data. They run daily, starting one day ago. The DAG should use Airflow 2.x imports, so I'll make sure to import DAG and the necessary operators correctly.

The get_user task uses an HTTP GET request to the reqres API. For that, I'll use the SimpleHttpOperator. The connection is specified as 'reqres', which is an HTTP connection. The response will be pushed to XCom so that the next task can process it.

Next, process_user is a PythonOperator that takes the response from get_user via XCom. It parses the JSON, extracts the required fields (first name, last name, email), and pushes them to XCom. I'll write a function for this that uses the task instance to pull the data.

The create_table task needs to create a PostgreSQL table. The user mentioned using a CustomPostgresOperator that extends PostgresOperator with template field support. So I'll define that custom operator first. It should have a template_fields attribute that includes 'sql' to allow templating. Then, in the DAG, this operator will execute the CREATE TABLE IF NOT EXISTS statement.

The insert_data task uses the same CustomPostgresOperator to insert data. The SQL here will be an INSERT statement, and the values should come from XCom variables pushed by process_user. Since the operator supports templating, I can use Jinja templates to pull the XCom values. The parameters would be from the task instance of process_user, so I'll use something like ti.xcom_pull(task_ids='process_user').

For dependencies, it's a linear flow, so each task is set to depend on the previous one using the >> operator.

Default args should include the start_date as days_ago(1), along with other standard parameters like retries and email settings. The DAG itself will have a daily schedule_interval and catchup set to False.

I need to make sure all imports are correct. The SimpleHttpOperator comes from airflow.providers.http.operators.http, and the PostgresOperator from airflow.providers.postgres.operators.postgres. Also, the PythonOperator is from airflow.operators.python.

Wait, the user mentioned using XCom between process_user and insert_data. So the process_user function should push the extracted fields to XCom, maybe as a dictionary. Then insert_data's SQL template can access these values using {{ ti.xcom_pull(task_ids='process_user') }}. But wait, the insert SQL would need to reference individual fields. Let me check: when process_user pushes, say, a dict with 'firstname', 'lastname', 'email', then in the SQL template, I can use something like 'VALUES (%s, %s, %s)' with the parameters pulled from XCom.

Wait, maybe the CustomPostgresOperator's SQL can be a templated field, so in the insert_data task, the SQL would be something like:

INSERT INTO users (firstname, lastname, email) VALUES (
    '{{ ti.xcom_pull(task_ids='process_user').get('firstname') }}',
    '{{ ti.xcom_pull(task_ids='process_user').get('lastname') }}',
    '{{ ti.xcom_pull(task_ids='process_user').get('email') }}'
)

But that might not be the best approach. Alternatively, the process_user task could push a tuple or a dictionary, and the insert_data task uses parameters in the SQL. Wait, the PostgresOperator can take parameters, but the CustomPostgresOperator is based on it. So maybe the SQL can have placeholders, and the parameters are passed via the 'parameters' argument, which can be templated.

Alternatively, perhaps the process_user pushes the data as a dictionary, and the insert_data task uses a SQL template with placeholders and parameters from XCom. For example:

sql = "INSERT INTO users (firstname, lastname, email) VALUES (%(firstname)s, %(lastname)s, %(email)s)"
parameters = ti.xcom_pull(task_ids='process_user')

Then, the operator would execute the SQL with the parameters. But how to pass that into the operator? Since the operator's parameters can be templated, maybe using the 'parameters' argument with a template that pulls the XCom data.

Hmm, maybe the insert_data task's parameters are set to a Jinja template that retrieves the XCom from process_user. Let me think. The CustomPostgresOperator has template_fields, so if parameters is a templated field, then in the operator, parameters can be set to "{{ ti.xcom_pull(task_ids='process_user') }}". But I'm not sure if the PostgresOperator's parameters are templated by default. The default template_fields for PostgresOperator are ['sql', 'parameters'], I believe. So the CustomPostgresOperator would inherit that if we set template_fields = PostgresOperator.template_fields + [...]? Wait, the user says the CustomPostgresOperator extends PostgresOperator with template field support. Maybe they meant that the custom operator adds more template fields, but in this case, perhaps the original PostgresOperator already supports templating for 'sql' and 'parameters'. So, in the insert_data task, the SQL would be a parameterized INSERT statement, and the parameters are pulled from XCom.

So, for the insert_data task:

sql = "INSERT INTO users (firstname, lastname, email) VALUES (%(firstname)s, %(lastname)s, %(email)s)"
parameters = {"firstname": ..., "lastname": ..., "email": ...}

But how to get the parameters from XCom. The process_user task pushes a dictionary with keys 'firstname', 'lastname', 'email'. So in the insert_data task's parameters, we can set it to a Jinja template that pulls the XCom value from process_user.

Wait, the parameters in PostgresOperator can be a dictionary, which can be templated if 'parameters' is in template_fields. So in the CustomPostgresOperator, if we include 'parameters' in template_fields, then we can set parameters to a Jinja expression that retrieves the XCom data.

But the original PostgresOperator's template_fields include 'parameters' already? Let me check. According to Airflow's documentation, the PostgresOperator has template_fields = ('sql', 'parameters', 'database', 'schema'). So yes, parameters is a templated field. Therefore, in the insert_data task, we can set parameters to a Jinja template that pulls the XCom data from process_user.

So, the insert_data task would have:

CustomPostgresOperator(
    task_id='insert_data',
    sql="INSERT INTO users (firstname, lastname, email) VALUES (%(firstname)s, %(lastname)s, %(email)s);",
    parameters="{{ ti.xcom_pull(task_ids='process_user') }}",
    postgres_conn_id='postgres'
)

Wait, but parameters expects a dictionary or list. When using Jinja, the value is a string, so if the XCom value is a dictionary, using "{{ ti.xcom_pull(...) }}" would render as a string representation of the dictionary, which might not be parsed correctly. Hmm, maybe that's an issue. Alternatively, perhaps the process_user task returns the dictionary, and the parameters are set to that via XCom.

Wait, the parameters field in PostgresOperator is a templated field, so when you set parameters to a Jinja template that returns a dictionary, it should work. Because when the template is rendered, it would be a Python dictionary. Let me test this in my mind. Suppose the process_user task returns {'firstname': 'John', 'lastname': 'Doe', 'email': 'john@example.com'}, then the parameters in insert_data would be that dictionary. So the rendered parameters would be a dictionary, and the SQL would have placeholders %(firstname)s etc., which would be correctly substituted.

Yes, that should work. So the insert_data task's parameters are set to the XCom value from process_user, which is a dictionary. Therefore, the parameters field in the CustomPostgresOperator can be a Jinja expression that pulls the XCom data.

So, in code:

parameters="{{ ti.xcom_pull(task_ids='process_user') }}"

But wait, in Jinja templating, when you use double curly braces, Airflow will render that as a string. However, if the XCom value is a dictionary, then when the template is rendered, it would be a string representation, which is not a dictionary anymore. That's a problem. For example, if the XCom value is a dict {'a': 1}, the rendered string would be "{'a': 1}", which is a string, not a dict. So using that as parameters would cause an error because the operator expects a dict or list.

Ah, right. So this approach won't work. Therefore, how to pass the parameters correctly?

Alternative approach: The process_user task can push each value individually to XCom with specific keys, and then the insert_data task can pull them using those keys. For example, in process_user:

ti.xcom_push(key='firstname', value=firstname)
ti.xcom_push(key='lastname', value=lastname)
ti.xcom_push(key='email', value=email)

Then, in the insert_data task's parameters, we can construct a dictionary using these keys:

parameters={
    'firstname': "{{ ti.xcom_pull(task_ids='process_user', key='firstname') }}",
    'lastname': "{{ ti.xcom_pull(task_ids='process_user', key='lastname') }}",
    'email': "{{ ti.xcom_pull(task_ids='process_user', key='email') }}"
}

But since parameters is a templated field, these Jinja expressions will be rendered as strings, which would be the actual values pulled from XCom. Wait, no. Because when you use Jinja templates in parameters, Airflow will render them as the actual values. So if the XCom value for 'firstname' is 'John', then the parameters would be a dictionary with 'firstname' as 'John', etc. That should work.

But how to structure the parameters in the operator. Let's see:

In the insert_data task:

sql = "INSERT INTO users (firstname, lastname, email) VALUES (%(firstname)s, %(lastname)s, %(email)s);"
parameters = {
    'firstname': "{{ ti.xcom_pull(task_ids='process_user', key='firstname') }}",
    'lastname': "{{ ti.xcom_pull(task_ids='process_user', key='lastname') }}",
    'email': "{{ ti.xcom_pull(task_ids='process_user', key='email') }}"
}

But when Airflow renders the parameters, each value will be replaced with the corresponding XCom value. However, the parameters are defined as a dictionary with values being Jinja template strings. When Airflow renders the template, these will be replaced with the actual values from XCom. So the rendered parameters will be a proper dictionary with the values, which can then be used in the SQL execution.

Yes, this should work. So the process_user task needs to push each field as a separate XCom key. Alternatively, push a single XCom value with a dictionary, but then in the parameters, we need to parse that. But that's more complicated. So perhaps pushing individual keys is easier.

So modifying the process_user function:

def process_user_data(ti, **kwargs):
    response = ti.xcom_pull(task_ids='get_user')
    data = json.loads(response)
    user = data.get('data', {})
    firstname = user.get('first_name', '')
    lastname = user.get('last_name', '')
    email = user.get('email', '')
    
    ti.xcom_push(key='firstname', value=firstname)
    ti.xcom_push(key='lastname', value=lastname)
    ti.xcom_push(key='email', value=email)

Then, in the insert_data task's parameters, we can reference each key:

parameters={
    'firstname': "{{ ti.xcom_pull(task_ids='process_user', key='firstname') }}",
    'lastname': "{{ ti.xcom_pull(task_ids='process_user', key='lastname') }}",
    'email': "{{ ti.xcom_pull(task_ids='process_user', key='email') }}"
}

But wait, the task_id for process_user is 'process_user', right? So in the parameters, the task_ids should be 'process_user'.

This way, each parameter is pulled from the XCom of the process_user task with the respective key.

Alternatively, if the process_user task returns a dictionary (by returning the value), then ti.xcom_pull would retrieve that dictionary. But then, in the parameters, we need to access the keys of that dictionary. However, in Jinja templates, accessing dictionary keys might be possible using dot notation or square brackets. For example, if the XCom value is a dict, then:

parameters="{{ ti.xcom_pull(task_ids='process_user') }}"

But this would render as the string representation of the dict, which is not usable. So perhaps pushing individual keys is better.

So, adjusting the process_user function to push each key separately.

Now, putting it all together.

First, the imports:

from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import json

Then, the CustomPostgresOperator. The user mentioned that it's extending PostgresOperator with template field support. Wait, the PostgresOperator already has template_fields including 'sql' and 'parameters', so maybe the custom operator isn't necessary. But the pipeline description says to use CustomPostgresOperator. So perhaps the user wants us to define it. The description says it extends PostgresOperator with template field support. Maybe adding more template fields, but in this case, perhaps the original PostgresOperator is sufficient. However, to follow the user's instruction, I need to define the CustomPostgresOperator.

So:

class CustomPostgresOperator(PostgresOperator):
    template_fields = PostgresOperator.template_fields + ('sql', 'parameters')

Wait, but PostgresOperator's template_fields already include 'sql' and 'parameters'. So adding them again is redundant. Maybe the user intended that the custom operator adds some other fields, but the description says "with template field support". Maybe the user just wants to show that a custom operator is used. So perhaps the code should include the CustomPostgresOperator even if it's the same as PostgresOperator, just to demonstrate.

So:

class CustomPostgresOperator(PostgresOperator):
    """Custom Postgres operator extending template fields"""
    template_fields = PostgresOperator.template_fields  # This includes 'sql', 'parameters', etc.

But perhaps the user wants to explicitly show that the custom operator is used. So defining it even if it's the same as the base class.

Now, the DAG definition:

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='user_processing_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define tasks

    get_user = SimpleHttpOperator(
        task_id='get_user',
        http_conn_id='reqres',
        endpoint='api/users/1',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        do_xcom_push=True,
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=process_user_data,
    )

    create_table = CustomPostgresOperator(
        task_id='create_table',
        sql="""CREATE TABLE IF NOT EXISTS users (
            firstname VARCHAR NOT NULL,
            lastname VARCHAR NOT NULL,
            email VARCHAR NOT NULL
        );""",
        postgres_conn_id='postgres',
    )

    insert_data = CustomPostgresOperator(
        task_id='insert_data',
        sql="""INSERT INTO users (firstname, lastname, email)
               VALUES (%(firstname)s, %(lastname)s, %(email)s);""",
        parameters={
            'firstname': "{{ ti.xcom_pull(task_ids='process_user', key='firstname') }}",
            'lastname': "{{ ti.xcom_pull(task_ids='process_user', key='lastname') }}",
            'email': "{{ ti.xcom_pull(task_ids='process_user', key='email') }}",
        },
        postgres_conn_id='postgres',
    )

    get_user >> process_user >> create_table >> insert_data

Wait, but the process_user task's function is called process_user_data, which needs to be defined. Let me write that function.

def process_user_data(ti, **kwargs):
    """Process API response and extract user data."""
    response = ti.xcom_pull(task_ids='get_user')
    # The response is already parsed by the response_filter in SimpleHttpOperator
    # Wait, in the get_user task, response_filter is set to lambda response: json.loads(response.text)
    # And do_xcom_push is True. So the XCom value is the result of response_filter, which is the parsed JSON.
    # So response here is the parsed JSON, not the raw text.

    user_data = response.get('data', {})
    firstname = user_data.get('first_name', '')
    lastname = user_data.get('last_name', '')
    email = user_data.get('email', '')

    # Push each field to XCom with specific keys
    ti.xcom_push(key='firstname', value=firstname)
    ti.xcom_push(key='lastname', value=lastname)
    ti.xcom_push(key='email', value=email)

So the process_user_data function pulls the XCom from get_user, which is the parsed JSON, extracts the fields, and pushes them to XCom with specific keys.

Then, the insert_data task's parameters reference these keys.

Putting all together, the code should be:

Imports, custom operator, DAG definition, tasks, dependencies.

Wait, also need to import timedelta for the retry_delay. Oh, right, in the default_args, 'retry_delay' is set to timedelta(minutes=5), so need to import timedelta from datetime.

So adding:

from datetime import timedelta

Now, putting all together:

The complete code would be:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import json


class CustomPostgresOperator(PostgresOperator):
    """Custom Postgres operator with extended template field support"""
    template_fields = PostgresOperator.template_fields


def process_user_data(ti, **kwargs):
    """Process API response and extract user information"""
    response = ti.xcom_pull(task_ids='get_user')
    user_info = response.get('data', {})
    
    ti.xcom_push(key='firstname', value=user_info.get('first_name', ''))
    ti.xcom_push(key='lastname', value=user_info.get('last_name', ''))
    ti.xcom_push(key='email', value=user_info.get('email', ''))


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='user_processing_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['user_processing'],
) as dag:

    get_user = SimpleHttpOperator(
        task_id='get_user',
        http_conn_id='reqres',
        endpoint='api/users/1',
        method='GET',
        response_filter=lambda response: response.json(),
        do_xcom_push=True,
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=process_user_data,
    )

    create_table = CustomPostgresOperator(
        task_id='create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                firstname VARCHAR NOT NULL,
                lastname VARCHAR NOT NULL,
                email VARCHAR NOT NULL
            );
        """,
        postgres_conn_id='postgres',
    )

    insert_data = CustomPostgresOperator(
        task_id='insert_data',
        sql="""
            INSERT INTO users (firstname, lastname, email)
            VALUES (%(firstname)s, %(lastname)s, %(email)s);
        """,
        parameters={
            'firstname': "{{ ti.xcom_pull(task_ids='process_user', key='firstname') }}",
            'lastname': "{{ ti.xcom_pull(task_ids='process_user', key='lastname') }}",
            'email': "{{ ti.xcom_pull(task_ids='process_user', key='email') }}",
        },
        postgres_conn_id='postgres',
    )

    get_user >> process_user >> create_table >> insert_data

Wait, in the SimpleHttpOperator, the response_filter was initially written as lambda response: json.loads(response.text), but using response.json() is better since the response object has a .json() method that parses the JSON. So that's correct.

Also, the parameters in insert_data are correctly referencing each XCom key from process_user.

This should handle the XCom passing between process_user and insert_data.

I think this covers all the requirements. The DAG is linear, uses the correct operators, defines the custom operator, and handles XCom properly.
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import json


class CustomPostgresOperator(PostgresOperator):
    """Custom Postgres operator with extended template field support"""
    template_fields = PostgresOperator.template_fields


def process_user_data(ti, **kwargs):
    """Process API response and extract user information"""
    response = ti.xcom_pull(task_ids='get_user')
    user_info = response.get('data', {})
    
    ti.xcom_push(key='firstname', value=user_info.get('first_name', ''))
    ti.xcom_push(key='lastname', value=user_info.get('last_name', ''))
    ti.xcom_push(key='email', value=user_info.get('email', ''))


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='user_processing_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['user_processing'],
) as dag:

    get_user = SimpleHttpOperator(
        task_id='get_user',
        http_conn_id='reqres',
        endpoint='api/users/1',
        method='GET',
        response_filter=lambda response: response.json(),
        do_xcom_push=True,
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=process_user_data,
    )

    create_table = CustomPostgresOperator(
        task_id='create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                firstname VARCHAR NOT NULL,
                lastname VARCHAR NOT NULL,
                email VARCHAR NOT NULL
            );
        """,
        postgres_conn_id='postgres',
    )

    insert_data = CustomPostgresOperator(
        task_id='insert_data',
        sql="""
            INSERT INTO users (firstname, lastname, email)
            VALUES (%(firstname)s, %(lastname)s, %(email)s);
        """,
        parameters={
            'firstname': "{{ ti.xcom_pull(task_ids='process_user', key='firstname') }}",
            'lastname': "{{ ti.xcom_pull(task_ids='process_user', key='lastname') }}",
            'email': "{{ ti.xcom_pull(task_ids='process_user', key='email') }}",
        },
        postgres_conn_id='postgres',
    )

    get_user >> process_user >> create_table >> insert_data