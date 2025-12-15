<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements.

First, the DAG ID is 'hive_script' and it's scheduled daily at 1:00 AM. The start date should be one day prior to the current date. So I'll set the start_date using datetime, maybe subtract one day from today. But wait, in Airflow, it's better to use a static start date. So perhaps set it to days_ago(1) or a fixed date. The user mentioned "one day prior to current date", so using days_ago(1) makes sense here.

The DAG has two tasks: a BashOperator and a HiveOperator. The BashOperator runs an echo command to print the username. Then the HiveOperator runs a Hive script that creates a database, table, and inserts a record. The tasks are sequential, so the Bash task runs first, then the Hive task.

For imports, I need to use Airflow 2.x style. So from airflow import DAG, and the operators from their respective modules. The BashOperator is in airflow.operators.bash, and HiveOperator is in airflow.providers.apache.hive.operators.hive. Wait, do I need to check the correct import path for HiveOperator? Yes, in Airflow 2.x, the HiveOperator is part of the apache.hive provider package. So the import should be from airflow.providers.apache.hive.operators.hive import HiveOperator.

The default_args should include the start_date, maybe other parameters like retries. The user didn't specify, but I'll include some sensible defaults. The DAG parameters should have schedule_interval set to '0 1 * * *' for daily at 1 AM, catchup=False to prevent backfilling.

The HiveOperator requires a hive_cli_conn_id, which is given as 'hive_local'. The SQL script for Hive should have the three statements: CREATE DATABASE IF NOT EXISTS, CREATE TABLE IF NOT EXISTS, and INSERT INTO. I'll write that as a multi-line string.

Now, putting it all together. Let's outline the code structure:

1. Import necessary modules.
2. Define default_args with owner, retries, etc.
3. Instantiate the DAG with the specified parameters.
4. Define the BashOperator task with the bash_command.
5. Define the HiveOperator task with the Hive script and connection.
6. Set dependencies using the >> operator.

Wait, the HiveOperator might require the hive connection to be set up. The user mentioned using 'hive_local' connection, so the task should have hive_cli_conn_id='hive_local'.

Also, the Hive script should be a single string with the three statements separated by semicolons or split into separate lines. Let me check Hive's syntax. Hive allows multiple statements if separated by semicolons, but sometimes the HiveOperator might require them to be in a list. Alternatively, using hiveconfs to allow multiple statements. Wait, the HiveOperator's hql parameter can take a string with multiple statements. So perhaps:

hql = """
CREATE DATABASE IF NOT EXISTS mydb;
CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
INSERT INTO mydb.test_af VALUES (2);
"""

But I need to make sure that the Hive server allows multi-statements. Alternatively, split into separate queries. But the user's description says it's a multi-statement Hive query, so combining them with semicolons should work.

Now, writing the code:

Import DAG and necessary operators. The HiveOperator is from airflow.providers.apache.hive.operators.hive. Also, need to import BashOperator.

The default_args should have start_date=days_ago(1), and other parameters like retries, maybe email on failure, etc. But the user didn't specify, so I'll add retries=1 for example.

Putting it all together, here's the code:

Make sure the code is PEP8 compliant, proper indentation, docstrings where needed. Also, the DAG is defined using the context manager.

Wait, the user said to use DAG context manager. So with DAG(...) as dag: and then define the tasks inside.

Yes, that's the recommended way.

So the code structure would be:

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hive_script',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['example'],
) as dag:

    run_after_loop = BashOperator(
        task_id='run_after_loop',
        bash_command='echo $(whoami)',
    )

    hive_script_task = HiveOperator(
        task_id='hive_script_task',
        hive_cli_conn_id='hive_local',
        hql="""
            CREATE DATABASE IF NOT EXISTS mydb;
            CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
            INSERT INTO mydb.test_af VALUES (2);
        """,
    )

    run_after_loop >> hive_script_task

Wait, I need to import timedelta for the retry_delay. Oh right, in the default_args, retry_delay is a timedelta. So I need to import that from datetime.

So adding:

from datetime import timedelta

But the user's code must be executable. So I need to include all necessary imports.

Also, the HiveOperator might require the hive provider to be installed. But the user said to assume required packages are installed.

Putting all together, here's the code with proper imports and structure.
</think>

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hive_script',
    default_args=default_args,
    description='Daily Hive operations pipeline',
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['hive', 'bash'],
) as dag:

    get_username = BashOperator(
        task_id='run_after_loop',
        bash_command='echo "Current user: $(whoami)"',
    )

    execute_hive_script = HiveOperator(
        task_id='hive_script_task',
        hive_cli_conn_id='hive_local',
        hql="""
            CREATE DATABASE IF NOT EXISTS mydb;
            CREATE TABLE IF NOT EXISTS mydb.test_af (test INT);
            INSERT INTO mydb.test_af VALUES (2);
        """,
    )

    get_username >> execute_hive_script