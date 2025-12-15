<think>
Okay, I need to create an Apache Airflow DAG based on the given pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline has four steps in a linear sequence: fetch HTML, extract and validate data, load to PostgreSQL, and conditional email alerts. Each task depends on the previous one. There's no branching or parallelism mentioned, except for the skip logic. But the user mentioned that tasks may skip execution if data exists or AQI is safe. However, since the example uses PythonOperator for all tasks, maybe the skip logic is handled within the functions.

The constraints specify using Airflow 2.x style imports. So I'll import DAG from airflow, PythonOperator, and maybe others. Since there's a PostgreSQL connection, I'll need the PostgresHook. Also, for email, maybe they use an EmailOperator, but the description says smtplib, so perhaps a PythonOperator for sending emails.

Wait, the user mentioned that if sensors are present, demonstrate proper use. But in the pipeline steps, there are no sensors described. So maybe no sensors here. The steps are all Python functions, so PythonOperators.

Default_args should include things like owner, start_date, maybe retries, etc. The schedule_interval could be None or a cron expression, but the example says specific schedule not shown, so maybe set schedule_interval as None. Catchup should be False.

Now, structuring the DAG with the context manager. Each task is a PythonOperator. The functions for each task need to be defined. Let's outline the functions:

1. fetch_mahidol_aqi_html: Uses requests to download the HTML and save it. Need to handle exceptions maybe, but the code should be executable. So inside the function, import requests, get the webpage, save to a file. But since Airflow tasks should have imports inside the function if using libraries not in the default environment.

Wait, but in Airflow, if you have external libraries, you need to make sure they're installed. The user mentioned using BeautifulSoup, requests, smtplib. So the functions will need to import those inside the callables to avoid top-level imports that might not be present.

Wait, no. The functions are defined in the DAG file. So the imports for requests, BeautifulSoup, etc., should be inside the functions, or at the top? If the DAG file is supposed to be standalone, then the imports need to be at the top. But the user says the code must be executable assuming required packages are installed. So I can include the necessary imports at the top.

Wait, but the user's constraints say to use proper imports. So, for example, if using BeautifulSoup, the import would be from bs4 import BeautifulSoup. So I need to make sure those are included.

But the problem is that when writing the DAG code, all the necessary imports should be present. So the code should have:

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import smtplib
from email.message import EmailMessage

But also, Airflow's modules. So the DAG file starts with:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

Wait, but the user's example uses smtplib, so maybe the email sending is done via a Python function using smtplib, not Airflow's EmailOperator. So the send_email_alert task will be a PythonOperator that uses smtplib.

Now, the functions:

For fetch_mahidol_aqi_html, the code would use requests.get to fetch the HTML from Mahidol's URL, then save it to a file. The URL is probably a placeholder, so I'll use a sample URL. Also, need to handle the file path. Maybe save to a temporary directory, but for the sake of the code, just write to a local file, like '/tmp/mahidol_aqi.html' or something. But in reality, the path should be configurable via variables, but the user says to avoid placeholders, so maybe hardcode a path, but that's not ideal. Alternatively, use a default path.

Next, extract_and_validate_data: reads the HTML file, parses with BeautifulSoup, extracts the data. Then checks data freshness (maybe the timestamp in the HTML is recent), checks for duplicates in the database. For checking duplicates, it would connect to PostgreSQL using PostgresHook. If data is fresh and not a duplicate, proceeds to create JSON output. Otherwise, the task should skip the subsequent steps? Or how is the skip logic handled? The user says tasks may skip execution if data exists. So perhaps in the extract_and_validate_data function, if the data is a duplicate, it raises an AirflowSkipException, which would mark the task as skipped, and downstream tasks would be skipped as well. But the pipeline steps mention that each task requires successful completion of the previous task. Wait, but the skip logic may cause tasks to be skipped. So perhaps, if the extract task determines that data is not new, it skips itself, which would skip downstream tasks.

But in Airflow, if a task is skipped, downstream tasks are skipped by default. So the code needs to handle that. So in the extract function, if data is duplicate, we raise AirflowSkipException. Then the load_data task would be skipped, as well as the email alert.

But how to implement that. So in the extract_and_validate_data function, after checking for duplicates, if it's a duplicate, we need to skip the task. So inside the function:

from airflow.exceptions import AirflowSkipException

...

def extract_and_validate_data(**kwargs):
    # read HTML file
    # parse data
    # check freshness and duplicates
    if duplicate:
        raise AirflowSkipException("Data is duplicate, skipping processing.")

Similarly, in the conditional_email_alert function, if AQI is below threshold, skip sending email.

But the problem is that the email task is after the load_data task. So if the load_data task is skipped, the email task would also be skipped. But according to the pipeline steps, the email is conditional based on AQI. So even if data is loaded, if AQI is safe, the email is not sent. So the email task's function would check the AQI value and decide to send or not.

So the email task's Python function would fetch the latest AQI from the database, check against thresholds, and send email if needed. If not needed, it could either do nothing or raise AirflowSkipException. But raising the exception would mark the task as skipped, which might be preferable.

Now, putting this together.

The DAG structure:

with DAG(...) as dag:

    fetch_html = PythonOperator(...)
    extract_data = PythonOperator(...)
    load_data = PythonOperator(...)
    send_alert = PythonOperator(...)

    fetch_html >> extract_data >> load_data >> send_alert

But the skip logic would affect the downstream tasks. For example, if extract_data skips, then load_data and send_alert are skipped. If load_data runs, then send_alert checks AQI and may skip.

Now, the functions:

For fetch_mahidol_aqi_html:

def fetch_mahidol_aqi_html():
    import requests
    url = "http://example.com/mahidol-aqi"  # placeholder URL
    response = requests.get(url)
    response.raise_for_status()  # raise HTTPError on bad status
    with open("/tmp/mahidol_aqi_latest.html", "w") as f:
        f.write(response.text)

But the URL is a placeholder. The user might expect a comment indicating to replace it, but the user said to avoid placeholders. So maybe use a realistic URL if known, but since I don't know the actual URL, perhaps use a sample. Alternatively, the code can have a comment indicating to set the correct URL.

But the user said to avoid placeholders like <TODO>, so perhaps just hardcode a URL, even if it's not real, as long as the code is syntactically correct.

For extract_and_validate_data:

def extract_and_validate_data(**kwargs):
    from bs4 import BeautifulSoup
    import json
    from datetime import datetime, timedelta
    from airflow.exceptions import AirflowSkipException

    # Read HTML file
    with open("/tmp/mahidol_aqi_latest.html", "r") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")
    # Extract data - example parsing, adjust based on actual HTML structure
    # Assuming data is in a table or specific elements
    # Placeholder extraction
    aqi_value = 50  # extracted from HTML
    timestamp = datetime.now()  # extracted from HTML

    # Check data freshness (e.g., within last hour)
    if datetime.now() - timestamp > timedelta(hours=1):
        raise AirflowSkipException("Data is not fresh.")

    # Check duplicates in PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM aqi_facts WHERE timestamp = %s)", (timestamp,))
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if exists:
        raise AirflowSkipException("Data already exists in database.")

    # Create JSON output
    data = {
        "timestamp": timestamp.isoformat(),
        "aqi": aqi_value,
        # other fields...
    }
    with open("/tmp/mahidol_aqi_processed.json", "w") as f:
        json.dump(data, f)

But this is a simplified version. The actual extraction would depend on the HTML structure. Also, the code assumes that the PostgresHook is set up with the connection ID "postgres_default", which the user should configure in Airflow.

For load_data_to_postgresql:

def load_data_to_postgresql(**kwargs):
    import json
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    with open("/tmp/mahidol_aqi_processed.json", "r") as f:
        data = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Example insert into multiple tables
    # Insert into datetime dimension
    cursor.execute(
        "INSERT INTO datetime_dim (timestamp) VALUES (%s) ON CONFLICT DO NOTHING",
        (data["timestamp"],)
    )
    # Insert into aqi_facts
    cursor.execute(
        "INSERT INTO aqi_facts (timestamp, aqi) VALUES (%s, %s)",
        (data["timestamp"], data["aqi"])
    )
    # Other inserts...
    conn.commit()
    cursor.close()
    conn.close()

For conditional_email_alert:

def conditional_email_alert(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import smtplib
    from email.message import EmailMessage
    from airflow.exceptions import AirflowSkipException

    # Fetch latest AQI from database
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT aqi FROM aqi_facts ORDER BY timestamp DESC LIMIT 1")
    latest_aqi = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    # Threshold check
    if latest_aqi < 100:  # Example threshold
        raise AirflowSkipException("AQI is within safe limits.")

    # Load email config (simplified)
    email_config = {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": "user@gmail.com",
        "sender_password": "password",
        "recipients": ["recipient@example.com"]
    }

    # Compose email
    msg = EmailMessage()
    msg.set_content(f"Alert: AQI level {latest_aqi} exceeds safe threshold")
    msg["Subject"] = "AQI Alert"
    msg["From"] = email_config["sender_email"]
    msg["To"] = ", ".join(email_config["recipients"])

    # Send email
    with smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"]) as server:
        server.starttls()
        server.login(email_config["sender_email"], email_config["sender_password"])
        server.send_message(msg)

But using hardcoded email credentials is bad practice. The user should use Airflow Connections or Variables to store these details. However, the user's code must be executable with minimal defaults, so perhaps include placeholders but mention in comments to configure them. But the user said to avoid placeholders like <TODO>, so maybe use example values but note that they should be replaced via Airflow variables or connections.

Alternatively, retrieve the email configuration from Airflow Variables. For example:

from airflow.models import Variable

email_config = Variable.get("email_config", deserialize_json=True)

But this requires that the Variable is set. However, the user's code should be executable, so perhaps handle the case where the Variable isn't set by providing defaults, but that's complicated. Alternatively, proceed with the hardcoded values but note that in a real scenario, they should be configured properly.

But the user's code must be executable, so perhaps proceed with the hardcoded values, even though they are not valid. The code will run but fail at runtime if the SMTP settings are incorrect. However, the user's requirement is to generate code that is syntactically correct and executable, not necessarily functional in production.

Putting all together, the DAG code would look like this:

Import necessary modules, define default_args, create the DAG with context manager, define the four tasks as PythonOperators, set dependencies, etc.

Now, checking for PEP 8 compliance: proper indentation, line lengths, etc.

Also, ensure that all imports are included. For example, the functions use requests, BeautifulSoup, smtplib, etc., so those imports should be at the top or inside the functions. Wait, no. If the functions are defined in the same module, the imports for requests, BeautifulSoup, etc., should be inside the functions to avoid top-level dependencies that may not be available in the Airflow environment. But the user said the code must be executable assuming required packages are installed. So, including the imports at the top is acceptable.

Wait, but if the code is written with top-level imports for requests, bs4, etc., then those need to be installed in the Airflow environment. The user's note says that external libraries are used, so the code should include those imports. So:

At the top of the module:

import json
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

But wait, some of these imports are only used inside the functions. For example, requests is used in fetch_mahidol_aqi_html. So, if the DAG file is parsed by Airflow, and the requests module is not installed, the DAG will fail to import. Therefore, it's better to have the imports inside the functions where they are used. Because Airflow only needs to parse the DAG file, and if the imports are inside the functions, the DAG can be parsed even if the modules are not installed (though the tasks will fail when run). However, the user's constraint says the code must be executable assuming required packages are installed. So including the top-level imports is acceptable.

But in practice, it's better to have the imports inside the functions to prevent top-level import errors. For example, if the DAG file is loaded and a top-level import is missing, the entire DAG won't load. By placing the imports inside the functions, the DAG can be loaded, and the task will fail only when executed if the import is missing.

But the user's code must be executable. So perhaps proceed with top-level imports, assuming that the required packages (requests, bs4, etc.) are installed in the Airflow environment.

So, the code starts with:

from datetime import datetime, timedelta
import json
import smtplib
from email.message import EmailMessage

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

Then, default_args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

Then, the DAG definition:

with DAG(
    dag_id='mahidol_aqi_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mahidol AQI data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['aqi', 'etl'],
) as dag:

Then, the task definitions:

    fetch_html = PythonOperator(
        task_id='fetch_mahidol_aqi_html',
        python_callable=fetch_mahidol_aqi_html,
    )

    extract_data = PythonOperator(
        task_id='extract_and_validate_data',
        python_callable=extract_and_validate_data,
    )

    load_data = PythonOperator(
        task_id='load_data_to_postgresql',
        python_callable=load_data_to_postgresql,
    )

    send_alert = PythonOperator(
        task_id='conditional_email_alert',
        python_callable=conditional_email_alert,
    )

    fetch_html >> extract_data >> load_data >> send_alert

Now, the functions:

def fetch_mahidol_aqi_html():
    url = "http://example.com/mahidol-aqi-report"
    response = requests.get(url)
    response.raise_for_status()
    with open("/tmp/mahidol_aqi_latest.html", "w") as f:
        f.write(response.text)

def extract_and_validate_data(**kwargs):
    with open("/tmp/mahidol_aqi_latest.html", "r") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")
    # Example extraction - adjust based on actual HTML structure
    aqi_element = soup.find("div", class_="aqi-value")
    aqi_value = int(aqi_element.text) if aqi_element else None
    timestamp_str = soup.find("span", class_="timestamp").text
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

    # Check data freshness (within 1 hour)
    if datetime.now() - timestamp > timedelta(hours=1):
        raise AirflowSkipException("Data is not fresh.")

    # Check duplicates
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM aqi_facts WHERE timestamp = %s)",
        (timestamp,)
    )
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if exists:
        raise AirflowSkipException("Duplicate data detected.")

    # Save structured data
    data = {
        "timestamp": timestamp.isoformat(),
        "aqi": aqi_value,
        "location": "Mahidol University"
    }
    with open("/tmp/mahidol_aqi_processed.json", "w") as f:
        json.dump(data, f)

def load_data_to_postgresql(**kwargs):
    with open("/tmp/mahidol_aqi_processed.json", "r") as f:
        data = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Insert into datetime dimension
    cursor.execute(
        """
        INSERT INTO datetime_dim (timestamp, hour, day, month, year)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (timestamp) DO NOTHING
        """,
        (
            data["timestamp"],
            data["timestamp"].hour,
            data["timestamp"].day,
            data["timestamp"].month,
            data["timestamp"].year,
        )
    )

    # Insert into aqi_facts
    cursor.execute(
        """
        INSERT INTO aqi_facts (timestamp, location_id, aqi)
        VALUES (%s, (SELECT id FROM location_dim WHERE name = %s), %s)
        """,
        (data["timestamp"], data["location"], data["aqi"])
    )

    conn.commit()
    cursor.close()
    conn.close()

def conditional_email_alert(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT aqi FROM aqi_facts ORDER BY timestamp DESC LIMIT 1"
    )
    result = cursor.fetchone()
    if not result:
        raise AirflowSkipException("No AQI data available.")
    latest_aqi = result[0]
    cursor.close()
    conn.close()

    if latest_aqi < 100:
        raise AirflowSkipException("AQI within safe threshold.")

    # Example email configuration - should use Airflow connections/variables
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "user@gmail.com"
    sender_password = "app_password"  # Use app-specific password
    recipients = ["alert@example.com"]

    msg = EmailMessage()
    msg.set_content(f"Air Quality Alert: AQI level {latest_aqi} exceeds safe limits.")
    msg["Subject"] = "AQI Alert Notification"
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipients)

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
    except Exception as e:
        raise Exception(f"Failed to send email alert: {e}")

Now, checking for PEP 8 compliance. Lines may be too long. For example, the SQL statements. But in Python, you can split strings into multiple lines with parentheses.

Also, ensure that all variables are properly defined. For example, in the extract function, the timestamp is parsed from the HTML. If the HTML doesn't have the expected elements, this would fail. But the user's code is supposed to be executable, so assuming that the HTML structure is as expected.

The code uses PostgresHook with connection ID 'postgres_default', which is the default ID. The user must have this connection set up in Airflow.

The email configuration uses hardcoded credentials, which is not secure. In practice, these should be stored in Airflow Connections or Variables, but the code uses placeholders for simplicity.

Finally, the DAG is structured with the four tasks in sequence. Each task raises AirflowSkipException under certain conditions, causing downstream tasks to be skipped.

This should meet the user's requirements: linear execution, proper imports, PEP 8 compliance, minimal defaults, and executable code.
</think>

from datetime import datetime, timedelta
import json
import smtplib
from email.message import EmailMessage

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_mahidol_aqi_html():
    url = "http://example.com/mahidol-aqi-report"
    response = requests.get(url)
    response.raise_for_status()
    with open("/tmp/mahidol_aqi_latest.html", "w") as f:
        f.write(response.text)

def extract_and_validate_data(**kwargs):
    with open("/tmp/mahidol_aqi_latest.html", "r") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")
    aqi_element = soup.find("div", class_="aqi-value")
    aqi_value = int(aqi_element.text) if aqi_element else None
    timestamp_str = soup.find("span", class_="timestamp").text
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

    if datetime.now() - timestamp > timedelta(hours=1):
        raise AirflowSkipException("Data is not fresh.")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM aqi_facts WHERE timestamp = %s)",
        (timestamp,)
    )
    exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if exists:
        raise AirflowSkipException("Duplicate data detected.")

    data = {
        "timestamp": timestamp.isoformat(),
        "aqi": aqi_value,
        "location": "Mahidol University"
    }
    with open("/tmp/mahidol_aqi_processed.json", "w") as f:
        json.dump(data, f)

def load_data_to_postgresql(**kwargs):
    with open("/tmp/mahidol_aqi_processed.json", "r") as f:
        data = json.load(f)
    data_time = datetime.fromisoformat(data["timestamp"])

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO datetime_dim (timestamp) VALUES (%s) ON CONFLICT DO NOTHING",
        (data_time,)
    )
    cursor.execute(
        """INSERT INTO aqi_facts (timestamp, location, aqi)
        VALUES (%s, %s, %s)""",
        (data_time, data["location"], data["aqi"])
    )
    conn.commit()
    cursor.close()
    conn.close()

def conditional_email_alert(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT aqi FROM aqi_facts ORDER BY timestamp DESC LIMIT 1")
    result = cursor.fetchone()
    if not result:
        raise AirflowSkipException("No AQI data available.")
    latest_aqi = result[0]
    cursor.close()
    conn.close()

    if latest_aqi < 100:
        raise AirflowSkipException("AQI within safe threshold.")

    msg = EmailMessage()
    msg.set_content(f"Air Quality Alert: AQI level {latest_aqi} exceeds safe limits.")
    msg["Subject"] = "AQI Alert Notification"
    msg["From"] = "alerts@example.com"
    msg["To"] = "recipient@example.com"

    try:
        with smtplib.SMTP("smtp.example.com", 587) as server:
            server.starttls()
            server.login("user", "pass")
            server.send_message(msg)
    except Exception as e:
        raise Exception(f"Failed to send email alert: {e}")

with DAG(
    dag_id='mahidol_aqi_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mahidol AQI data',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['aqi', 'etl'],
) as dag:

    fetch_html = PythonOperator(
        task_id='fetch_mahidol_aqi_html',
        python_callable=fetch_mahidol_aqi_html,
    )

    extract_data = PythonOperator(
        task_id='extract_and_validate_data',
        python_callable=extract_and_validate_data,
    )

    load_data = PythonOperator(
        task_id='load_data_to_postgresql',
        python_callable=load_data_to_postgresql,
    )

    send_alert = PythonOperator(
        task_id='conditional_email_alert',
        python_callable=conditional_email_alert,
    )

    fetch_html >> extract_data >> load_data >> send_alert