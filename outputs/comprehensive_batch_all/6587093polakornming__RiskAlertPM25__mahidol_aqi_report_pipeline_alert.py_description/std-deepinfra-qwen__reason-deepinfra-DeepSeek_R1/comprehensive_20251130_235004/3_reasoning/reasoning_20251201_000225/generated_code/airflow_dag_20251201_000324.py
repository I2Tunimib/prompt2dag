<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements and constraints.

First, the pipeline has four steps: fetching HTML, extracting data, loading to PostgreSQL, and conditional email alerts. All tasks are sequential with no branching or parallelism. So the dependencies will be linear.

The user specified Airflow 2.x style imports. So I'll use the correct modules like DAG from airflow, PythonOperator, and maybe others. Since there's a database involved, I'll need the PostgresHook. Also, for email, maybe the EmailOperator, but the description mentions using smtplib, so perhaps a PythonOperator for that task.

Wait, the pipeline uses SMTP (Gmail), so the email sending is custom. So I'll have to create a Python function that uses smtplib and attach it via PythonOperator.

Looking at the steps:

1. Fetch Mahidol AQI HTML: This is an HTTP request to get the webpage. So I'll use requests.get in a Python function. Save the HTML locally. Need to handle possible errors here.

2. Extract and Validate Data: Parse HTML with BeautifulSoup. Extract metrics, validate freshness, check duplicates against the DB. So this task will involve querying the PostgreSQL database to check if the data already exists. For that, I'll use PostgresHook to connect. If data is fresh and not a duplicate, proceed; else, skip subsequent tasks? Or skip just this task? The description says "Skip Logic: Tasks include conditional checks that may skip execution if data already exists or AQI levels are safe." So each task has its own checks. Hmm, maybe the extract task checks for duplicates and if found, skips the rest. But the problem is tasks are linear. Wait, the pipeline steps are linear, but each task may have skip logic internally. So perhaps each task has a condition to decide whether to proceed or not. For example, the extract task might check if the data is already present, and if so, skip processing, which would mean the next tasks (load and alert) might not run. But in Airflow, tasks are dependent on the previous task's success. So if a task is skipped, the downstream tasks would still run unless we use branching. But the description says "linear sequential execution with no branching", so maybe each task has logic to do nothing if not needed, but the task itself completes successfully regardless. For example, in the extract task, if data is duplicate, it doesn't process but the task succeeds, allowing the next tasks to run. But the next tasks would then check again? Or perhaps the extract task creates a JSON output only if data is valid and not duplicate. Then the load task would check if the JSON exists and proceed. Hmm, maybe the tasks pass information via XComs or files, and each task checks if the necessary data is present. Alternatively, each task has a condition to execute its main logic or not. But since the user mentioned "skip logic" in tasks, perhaps each task has internal checks and may decide to do nothing, but the task itself is considered successful. So the dependencies are linear, but tasks may skip their processing based on conditions.

For the DAG structure:

- default_args should include things like owner, start_date, maybe retries, etc. The user didn't specify, so I'll add minimal defaults.

- The DAG should have schedule_interval, perhaps None if not specified, and catchup=False.

Now, let's outline the tasks:

1. fetch_html_task: PythonOperator that uses requests to download HTML and save to a file. The function should handle exceptions, maybe, but if it fails, the task fails.

2. extract_data_task: PythonOperator that parses the HTML, checks for duplicates in the DB. If data is fresh and not a duplicate, creates JSON. Otherwise, maybe the JSON isn't created, and the next task checks for its existence.

3. load_data_task: PythonOperator that reads the JSON, transforms data, and loads into PostgreSQL using PostgresHook. If the JSON isn't present (because extract skipped), this task could check and skip.

4. send_email_alert_task: PythonOperator that checks the AQI from the loaded data (or from XComs) and sends an email if thresholds are exceeded. If AQI is safe, the task does nothing.

Wait, but how does the send_email task get the AQI value? Maybe the extract task stores it in XCom, or the load task does. Alternatively, the email task queries the database for the latest AQI. Hmm, perhaps the load task stores the AQI value in XCom, which the email task then pulls.

Alternatively, each task passes data via XComs. For example, the extract task could push the AQI value to XCom, which the email task uses. But if the extract task skips processing (because data is duplicate), then there's no AQI value. So maybe the email task checks the database for the latest entry.

But this complicates things. Alternatively, the email task could read the JSON file created by the extract task. If the JSON wasn't created (because data was duplicate), then the email task skips sending. But how to handle that.

Alternatively, each task is designed to proceed only if the previous step produced the necessary data. So, for example, the extract task only produces JSON if data is new. Then the load task checks if the JSON exists. If not, it skips. Similarly, the email task checks if the data was loaded and then proceeds.

But in Airflow, tasks are supposed to succeed or fail. Skipping is done via branching. However, the user mentioned "linear sequential execution with no branching", so perhaps each task proceeds regardless but internally may not perform actions if conditions aren't met. So the tasks always succeed but may have no effect.

Alternatively, maybe the extract task raises an AirflowSkipException if the data is duplicate, which would mark the task as skipped, and downstream tasks would be skipped as well. But the user's description says "linear sequential execution" and "strict linear dependency where each task requires successful completion of the previous task". So if a task is skipped, the next tasks wouldn't run. Therefore, perhaps the tasks shouldn't skip but instead proceed, but internally decide whether to perform their action. For example, the extract task checks for duplicates, and if found, doesn't create the JSON file. Then the load task checks if the JSON exists; if not, it does nothing. Similarly, the email task checks if the AQI in the database meets the threshold.

But how to track whether the data was loaded. Maybe the load task pushes a flag to XCom indicating success, which the email task checks. Alternatively, the email task always runs, queries the latest AQI from the database, and sends an email if needed.

This seems feasible. So:

- fetch_html: always runs, saves HTML.

- extract_data: parses HTML, checks DB for duplicates. If not duplicate, saves JSON. Otherwise, no JSON.

- load_data: if JSON exists, loads into DB. Else, does nothing.

- send_email: checks DB for latest AQI, sends email if needed.

This way, even if extract_data doesn't produce JSON (because data is duplicate), load_data does nothing, and send_email still checks the latest AQI (which might be from a previous run). But the problem is that the email alert is based on the newly loaded data. If the data wasn't loaded because it's a duplicate, then the email should check the existing data. But the pipeline's purpose is to send alerts based on the current data, which might be the same as before. So maybe the email task should always check the latest data in the DB, regardless of whether new data was loaded.

So the email task would query the latest AQI value from the database and decide whether to send an alert.

Now, let's outline the code.

First, imports:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import smtplib
from email.message import EmailMessage

Then default_args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

Define the DAG:

with DAG(
    'mahidol_aqi_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mahidol AQI data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['aqi'],
) as dag:

Then the tasks.

First task: fetch_html.

def fetch_html(**kwargs):
    url = 'http://example.com/mahidol-aqi-report'  # Replace with actual URL
    response = requests.get(url)
    response.raise_for_status()
    with open('/path/to/aqi_report.html', 'w') as f:
        f.write(response.text)
    # Note: File path should be configurable, perhaps via Variable.

But hardcoding paths isn't ideal. Maybe use a variable or a config. But for the sake of example, proceed.

Second task: extract_data.

def extract_data(**kwargs):
    # Load HTML file
    with open('/path/to/aqi_report.html', 'r') as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')
    # Extract data from HTML - example parsing, adjust as needed
    # Assuming data is in certain elements; this is placeholder
    aqi_value = soup.find('div', class_='aqi-value').text.strip()
    timestamp = soup.find('div', class_='timestamp').text.strip()
    location = soup.find('div', class_='location').text.strip()
    
    # Check data freshness (example: timestamp within last 1 hour)
    report_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    if (datetime.now() - report_time) > timedelta(hours=1):
        raise ValueError('Data is not fresh')
    
    # Check for duplicates in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM aqi_facts WHERE timestamp = %s", (report_time,))
    count = cursor.fetchone()[0]
    if count > 0:
        print("Duplicate data found, skipping processing.")
        return  # No JSON output
    
    # Create structured JSON
    data = {
        'timestamp': timestamp,
        'location': location,
        'aqi': aqi_value,
        # Other metrics as needed
    }
    with open('/path/to/aqi_data.json', 'w') as f:
        json.dump(data, f)

Third task: load_data.

def load_data(**kwargs):
    # Check if JSON exists
    try:
        with open('/path/to/aqi_data.json', 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("No data to load.")
        return
    
    # Transform and load into PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    # Insert into datetime table
    cursor.execute(
        "INSERT INTO datetime (timestamp) VALUES (%s) ON CONFLICT DO NOTHING",
        (data['timestamp'],)
    )
    # Insert into location table
    cursor.execute(
        "INSERT INTO location (name) VALUES (%s) ON CONFLICT DO NOTHING",
        (data['location'],)
    )
    # Insert into pollution dimensions (example)
    # Assuming some mapping from AQI to pollution levels
    # ...
    # Insert into aqi_facts
    cursor.execute(
        """INSERT INTO aqi_facts (timestamp_id, location_id, aqi_value)
           SELECT d.id, l.id, %s
           FROM datetime d, location l
           WHERE d.timestamp = %s AND l.name = %s""",
        (data['aqi'], data['timestamp'], data['location'])
    )
    connection.commit()

Fourth task: send_email_alert.

def send_email_alert(**kwargs):
    # Get latest AQI from PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT aqi_value FROM aqi_facts ORDER BY timestamp_id DESC LIMIT 1")
    latest_aqi = cursor.fetchone()
    if not latest_aqi:
        print("No AQI data found.")
        return
    latest_aqi = latest_aqi[0]
    
    # Threshold check
    if latest_aqi > 100:  # Example threshold
        # Load email config
        with open('/path/to/email_config.json') as f:
            email_config = json.load(f)
        
        msg = EmailMessage()
        msg.set_content(f"AQI level {latest_aqi} exceeds safe threshold.")
        msg['Subject'] = 'AQI Alert'
        msg['From'] = email_config['sender']
        msg['To'] = ', '.join(email_config['recipients'])
        
        # Send email
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(email_config['sender'], email_config['password'])
            server.send_message(msg)
        print("Email alert sent.")
    else:
        print("AQI level is safe.")

Now, create the tasks in the DAG:

fetch_html_task = PythonOperator(
    task_id='fetch_mahidol_aqi_html',
    python_callable=fetch_html,
)

extract_data_task = PythonOperator(
    task_id='extract_and_validate_data',
    python_callable=extract_data,
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgresql',
    python_callable=load_data,
)

send_email_alert_task = PythonOperator(
    task_id='conditional_email_alert',
    python_callable=send_email_alert,
)

Set dependencies:

fetch_html_task >> extract_data_task >> load_data_task >> send_email_alert_task

But wait, the email task depends on load_data_task, but the email task checks the latest AQI in the DB regardless of whether new data was loaded. So even if load_data_task didn't insert anything (because data was duplicate), the email task would still check the latest entry. That's correct.

Now, some considerations:

- File paths are hardcoded. In a real scenario, these should be configurable via Airflow Variables or templates.

- The HTML parsing logic is simplified; the actual implementation would need to adjust to the structure of Mahidol's website.

- The email_config.json should contain sender email, password, and recipients. Using a JSON file is okay, but in practice, sensitive data like passwords should be stored in Airflow Connections or Variables encrypted.

- The PostgresHook uses 'postgres_default' connection ID, which should be set up in Airflow.

- Error handling: The current functions may raise exceptions, which would cause task failures. Retries are set in default_args.

- The data freshness check in extract_data raises ValueError if data is older than an hour. This would fail the task. But according to the pipeline description, tasks may skip execution if data is not fresh. So perhaps instead of raising an error, the task should skip processing (i.e., not create JSON) and log a message. But raising an error would cause the task to fail, which may not be desired. Alternatively, the task could check freshness and if not fresh, skip processing (no JSON) but still return success. So modify the extract_data function:

In the extract_data function:

report_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
if (datetime.now() - report_time) > timedelta(hours=1):
    print("Data is not fresh, skipping processing.")
    return  # No JSON output

Similarly, for the duplicate check.

This way, the task completes successfully but doesn't produce JSON if data is not fresh or duplicate.

Thus, the load_data_task will check for the JSON and proceed only if it exists.

This approach ensures that tasks succeed even if they skip processing, maintaining the linear dependency.

So, the code should be adjusted accordingly.

Another point: The send_email_alert_task uses smtplib with a password in a JSON file. This is not secure. In practice, use Airflow's EmailOperator with SMTP connection configured, or retrieve credentials from a secure source. But the user specified to use smtplib, so proceed as per description.

Putting it all together, the code would look like the following.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import smtplib
from email.message import EmailMessage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mahidol_aqi_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mahidol AQI data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['aqi'],
) as dag:

    def fetch_html(**kwargs):
        url = 'http://example.com/mahidol-aqi-report'
        html_path = '/tmp/aqi_report.html'
        
        response = requests.get(url)
        response.raise_for_status()
        
        with open(html_path, 'w') as f:
            f.write(response.text)
        kwargs['ti'].xcom_push(key='html_path', value=html_path)

    def extract_data(**kwargs):
        ti = kwargs['ti']
        html_path = ti.xcom_pull(task_ids='fetch_mahidol_aqi_html', key='html_path')
        json_path = '/tmp/aqi_data.json'
        
        with open(html_path) as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        # Example extraction - adjust based on actual HTML structure
        aqi_value = float(soup.find('span', {'class': 'aqi-value'}).text)
        timestamp_str = soup.find('time').attrs['datetime']
        location = soup.find('h1', {'class': 'location'}).text.strip()
        
        report_time = datetime.fromisoformat(timestamp_str)
        if datetime.now() - report_time > timedelta(hours=1):
            print('Data is older than 1 hour')
            return

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM aqi_facts WHERE timestamp = %s", (report_time,))
            if cur.fetchone():
                print('Duplicate data found')
                return

        data = {
            'timestamp': report_time.isoformat(),
            'location': location,
            'aqi': aqi_value
        }
        with open(json_path, 'w') as f:
            json.dump(data, f)
        ti.xcom_push(key='json_path', value=json_path)

    def load_data(**kwargs):
        ti = kwargs['ti']
        json_path = ti.xcom_pull(task_ids='extract_and_validate_data', key='json_path')
        
        try:
            with open(json_path) as f:
                data = json.load(f)
        except FileNotFoundError:
            print('No valid data to load')
            return

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            # Insert datetime
            cur.execute("""
                INSERT INTO datetime (timestamp)
                VALUES (%s)
                ON CONFLICT (timestamp) DO NOTHING
                RETURNING id
            """, (data['timestamp'],))
            datetime_id = cur.fetchone()[0] if cur.rowcount else None
            
            # Insert location
            cur.execute("""
                INSERT INTO location (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
            """, (data['location'],))
            location_id = cur.fetchone()[0] if cur.rowcount else None
            
            # Insert AQI fact
            if datetime_id and location_id:
                cur.execute("""
                    INSERT INTO aqi_facts (datetime_id, location_id, aqi_value)
                    VALUES (%s, %s, %s)
                """, (datetime_id, location_id, data['aqi']))
            
            conn.commit()

    def send_alert(**kwargs):
        ti = kwargs['ti']
        json_path = ti.xcom_pull(task_ids='extract_and_validate_data', key='json_path')
        
        try:
            with open(json_path) as f:
                data = json.load(f)
            aqi = data['aqi']
        except (FileNotFoundError, TypeError):
            print('No new data to evaluate')
            return

        if aqi > 150:
            with open('/config/email_settings.json') as f:
                email_conf = json.load(f)
            
            msg = EmailMessage()
            msg['Subject'] = f'AQI Alert: {aqi}'
            msg['From'] = email_conf['sender']
            msg['To'] = ', '.join(email_conf['recipients'])
            msg.set_content(f'AQI level {aqi} exceeds safety threshold at {data["location"]}')
            
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(email_conf['sender'], email_conf['password'])
                server.send_message(msg)

    fetch_html_task = PythonOperator(
        task_id='fetch_mahidol_aqi_html',
        python_callable=fetch_html
    )

    extract_data_task = PythonOperator(
        task_id='extract_and_validate_data',
        python_callable=extract_data
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_postgresql',
        python_callable=load_data
    )

    send_alert_task = PythonOperator(
        task_id='conditional_email_alert',
        python_callable=send_alert
    )

    fetch_html_task >> extract_data_task >> load_data_task >> send_alert_task