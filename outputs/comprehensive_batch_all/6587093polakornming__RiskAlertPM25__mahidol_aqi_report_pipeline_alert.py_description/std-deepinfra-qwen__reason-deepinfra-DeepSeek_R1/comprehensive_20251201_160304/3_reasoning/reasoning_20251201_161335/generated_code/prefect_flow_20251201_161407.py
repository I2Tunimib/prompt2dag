<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the steps involved. The pipeline has four main steps: fetching HTML, extracting and validating data, loading to PostgreSQL, and sending conditional email alerts. The orchestration is linear, so each task depends on the previous one.

First, I'll set up the necessary imports. The user mentioned using Prefect's flow and task decorators, so I'll import those. The dependencies include requests, BeautifulSoup, psycopg2, smtplib, etc. I'll make sure to include those imports as well.

Each step becomes a Prefect task. The flow will be the main function that calls these tasks in order. Since the execution is sequential, I don't need to use .submit() for parallel tasks. Each task function should have appropriate parameters and return values to pass data along.

For the "Fetch Mahidol AQI HTML" task, I'll use requests to get the HTML content and save it to a file. Need to handle exceptions here, maybe just let Prefect catch them as task failures.

Next, the "Extract and Validate Data" task will parse the HTML using BeautifulSoup. It needs to extract metrics, validate freshness, check for duplicates. I'll need to read the HTML file, parse it, then perform the checks. If data is duplicate or not fresh, the task might return None, and subsequent tasks can handle that.

The "Load Data to PostgreSQL" task will take the extracted data and insert it into the database. Using psycopg2 to connect, create tables if they don't exist, and insert data. Need to structure the data into the appropriate tables: datetime, location, pollution, and aqi_facts.

The "Conditional Email Alert" task checks the AQI value against thresholds. If it's exceeded, send an email via SMTP. The task should only run if the data was loaded successfully, so it depends on the previous task's output.

Now, considering the Prefect specifics. Each task is decorated with @task. The flow is decorated with @flow. The main function calls each task in order, passing the outputs as needed. Since the steps are sequential, each task is called normally without .submit().

Configuration like database credentials and email settings should be handled via Prefect blocks or environment variables, but the user didn't mention that, so I'll hardcode them with placeholders, adding comments that they should be configured properly.

I need to include docstrings for each task and the flow. Also, ensure PEP8 compliance, so line lengths, proper indentation, etc.

The if __name__ == '__main__' guard will call the flow function for local execution.

For the database connection, using psycopg2 directly. The user mentioned Airflow's PostgresHook, but since we're using Prefect, it's better to use the standard Postgres connector or Prefect's SQLAlchemy integration. However, to keep it simple, I'll use psycopg2 with a connection string.

Email configuration: The task uses smtplib. Again, credentials are placeholders. In a real scenario, these should be securely managed.

Error handling: Each task may raise exceptions, which Prefect will handle by marking the task as failed. The flow will stop if a task fails, which aligns with the described strict dependencies.

Scheduling: The user mentioned it's configured via Airflow, but in Prefect, scheduling is done through deployments. I'll add a comment about deployment configuration.

Testing if all tasks are connected properly. The output of fetch_html is passed to extract_data, which is then passed to load_data, and finally to send_alert. If any task returns None, the next task should handle it. For example, if extract_data finds duplicates, it returns None, and load_data might check for that and skip.

Wait, but in the pipeline description, the tasks have skip logic. So, if data is duplicate, extract_data might skip processing. So, the extract_data task would return the data only if it's valid and not a duplicate. Otherwise, maybe return None, and the load_data task would check if data is present before proceeding. Similarly, the send_alert task would check if the AQI is above threshold.

So in code, after extracting data, if it's None, the load task skips. Similarly, the send_alert task checks the AQI value.

But how to pass the AQI value from the extracted data to the send_alert task? The load_data task would need to return the AQI value, or the data needs to be passed through.

Alternatively, the extract_data task returns the structured data including AQI, which is then passed through load_data (which might return the AQI or a flag), and then send_alert uses that.

Hmm. Let's structure it such that extract_data returns the processed data, including AQI. Then load_data takes that data, inserts into DB, and returns the AQI value. Then send_alert uses that AQI value to decide whether to send an email.

Alternatively, the data is passed along each task. So:

fetch_html returns the HTML file path.

extract_data takes the HTML path, processes it, returns the data (as a dict with AQI, etc.).

load_data takes the data, inserts into DB, returns the data or just the AQI.

send_alert takes the AQI value.

So in code:

data = extract_data(html_path)

if data is not None:

    load_result = load_data(data)

    aqi = data['aqi']

    send_alert(aqi)

But in Prefect, each task is called in sequence. So within the flow function:

html_path = fetch_mahidol_aqi_html()

extracted_data = extract_and_validate_data(html_path)

if extracted_data:

    load_data_to_postgresql(extracted_data)

    if extracted_data['aqi'] > threshold:

        conditional_email_alert(extracted_data['aqi'])

Wait, but tasks are supposed to be independent. So maybe the send_alert task should take the AQI value as a parameter, which comes from the extracted data. But how to get that value after load_data? Because the load_data task might have transformed the data.

Alternatively, the extracted_data includes the AQI, and after loading, the send_alert task is called with that AQI.

But in the flow, after extracting, you load, then send alert based on the extracted data's AQI.

But the load task might not return anything. So perhaps the flow is:

html_path = fetch_mahidol_aqi_html()

extracted_data = extract_and_validate_data(html_path)

if extracted_data is not None:

    load_data_to_postgresql(extracted_data)

    if extracted_data['aqi'] > 100:  # example threshold

        conditional_email_alert(extracted_data['aqi'])

But in Prefect, tasks are part of the flow's DAG. So the send_alert task should be a task that's called conditionally. However, Prefect flows can have conditional logic based on task outputs.

So in the flow function, after extracting data, check if it's not None, then proceed to load, then check AQI and send alert.

But how to structure that within the flow. Let me draft the code.

The flow function would look like:

@flow
def aqi_etl_pipeline():
    html_path = fetch_mahidol_aqi_html()
    extracted_data = extract_and_validate_data(html_path)
    if extracted_data:
        load_data_to_postgresql(extracted_data)
        if extracted_data['aqi'] > 100:
            conditional_email_alert(aqi_value=extracted_data['aqi'])

But wait, the extracted_data might be a dictionary with more info. So the 'aqi' key should be present.

Now, for each task:

The fetch task gets the HTML and saves it, returns the path.

The extract task parses it, checks for duplicates (by querying the DB?), validates freshness (maybe by timestamp in the data), and returns the structured data or None.

The load task takes the data and inserts into PostgreSQL.

The send alert task sends email if AQI is above threshold.

Now, implementing the extract task's duplicate check. How does it check against the database? It would need to connect to the DB and query existing records. So maybe in the extract task, after parsing the HTML, it checks if the data is already present in the DB. If yes, returns None; else returns the data.

Alternatively, the load task checks for duplicates before inserting. But according to the pipeline description, the extract step includes checking for duplicates. So the extract task would be responsible for that.

So in the extract_and_validate_data task, after parsing, it connects to the DB and checks if the data (e.g., based on timestamp) already exists. If it does, return None; else return the data.

This requires the extract task to have access to the DB connection details. So in the code, the extract task would need to connect to PostgreSQL to perform the check.

Similarly, the load task would then insert the data, assuming it's not a duplicate.

But this could lead to a race condition if another process inserts the same data between the extract check and the load. But given the pipeline's linear execution, maybe it's acceptable.

Alternatively, the load task uses a unique constraint in the database and handles the insert error, but the pipeline includes the check in the extract step to skip processing.

So, in code, the extract task would have a function that, after parsing, queries the database to see if the data's timestamp already exists. If yes, return None; else return the data.

This requires the extract task to have a DB connection. So, in the task function, we'd have code to connect to PostgreSQL.

But how to manage the DB connection parameters? For simplicity, I'll hardcode them with placeholders, noting that they should be configured properly in a real setup.

Putting it all together, the code would have:

- fetch_mahidol_aqi_html task: uses requests to get HTML, saves to a file, returns the path.
- extract_and_validate_data task: reads the HTML file, parses with BeautifulSoup, extracts data, checks DB for duplicates, returns data or None.
- load_data_to_postgresql task: takes the data, connects to DB, inserts into tables.
- conditional_email_alert task: sends email if AQI is above threshold.

Now, implementing each task.

For the extract task, after parsing, let's say the data includes a timestamp. The task checks the database's datetime table to see if that timestamp exists. If it does, the data is a duplicate, so return None.

The load task then inserts the data into the respective tables, assuming it's not a duplicate.

The send_alert task uses smtplib to send an email. The email configuration (sender, password, recipients) is hardcoded with placeholders.

Potential issues: handling DB connections properly, closing them after use. Using context managers for connections and cursors.

Testing if all the tasks are properly structured. Also, ensuring that if the extract task returns None, the load and send tasks are skipped.

In the flow function, after calling extract_and_validate_data, we check if the returned data is not None. If so, proceed to load and then check AQI.

Now, code structure:

import statements.

Then the tasks, each decorated with @task.

Then the flow function.

Finally, the if __name__ block.

Need to make sure that all variables are properly passed between tasks.

Another consideration: the data extracted from HTML. For example, the structure might include location, datetime, pollution metrics, and AQI. The extract task would parse these and return a dictionary with keys like 'timestamp', 'location', 'pm25', 'aqi', etc.

The load task would then split this data into the respective tables.

For example, inserting into datetime_dimension (datetime_id, timestamp, etc.), location_dimension, pollution_dimension, and aqi_facts.

But the exact schema isn't provided, so I'll make assumptions. The code will have SQL statements to create tables if they don't exist, and insert data.

The email task uses a threshold (e.g., 100) to decide whether to send an alert.

Putting all together, here's the code.
</think>

import json
from datetime import datetime
import smtplib
from email.message import EmailMessage
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import psycopg2
from prefect import flow, task

DB_CONFIG = {
    "host": "localhost",
    "database": "aqi",
    "user": "postgres",
    "password": "postgres"
}

EMAIL_CONFIG = {
    "sender": "user@gmail.com",
    "password": "apppassword",
    "recipients": ["admin@example.com"]
}

@task(retries=2, retry_delay_seconds=10)
def fetch_mahidol_aqi_html() -> Path:
    """Fetch Mahidol AQI report HTML and save locally"""
    url = "https://www.example-mahidol-aqi-report.edu"
    response = requests.get(url)
    response.raise_for_status()
    
    output_path = Path(f"mahidol_aqi_{datetime.now().isoformat()}.html")
    output_path.write_text(response.text)
    return output_path

@task
def extract_and_validate_data(html_path: Path) -> dict | None:
    """Extract AQI data from HTML with validation and deduplication checks"""
    soup = BeautifulSoup(html_path.read_text(), "html.parser")
    
    # Example extraction logic - implement actual parsing based on site structure
    extracted_data = {
        "timestamp": datetime.now().isoformat(),
        "location": "Bangkok",
        "pm25": 35.2,
        "pm10": 50.1,
        "aqi": 78
    }
    
    # Check for duplicates in database
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM datetime_dimension WHERE timestamp = %s",
                            (extracted_data["timestamp"],))
                if cur.fetchone():
                    return None
    except Exception as e:
        print(f"Database check failed: {e}")
        raise
    
    return extracted_data

@task
def load_data_to_postgresql(data: dict) -> None:
    """Load transformed data into PostgreSQL data warehouse"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Create tables if not exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS datetime_dimension (
                        datetime_id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP UNIQUE
                    )""")
                
                cur.execute("""
                    INSERT INTO datetime_dimension (timestamp)
                    VALUES (%s)
                    ON CONFLICT DO NOTHING
                    RETURNING datetime_id""", (data["timestamp"],))
                
                # Similar inserts for other dimensions and facts
                conn.commit()
    except Exception as e:
        print(f"Data load failed: {e}")
        raise

@task
def conditional_email_alert(aqi_value: float) -> None:
    """Send email alert if AQI exceeds safety threshold"""
    if aqi_value < 100:
        return

    msg = EmailMessage()
    msg["Subject"] = "AQI Safety Alert"
    msg["From"] = EMAIL_CONFIG["sender"]
    msg["To"] = ", ".join(EMAIL_CONFIG["recipients"])
    msg.set_content(f"Current AQI level {aqi_value} exceeds safety threshold!")
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
            server.send_message(msg)
    except Exception as e:
        print(f"Email alert failed: {e}")
        raise

@flow
def aqi_etl_pipeline():
    """Main ETL pipeline for Mahidol AQI data processing"""
    html_path = fetch_mahidol_aqi_html()
    extracted_data = extract_and_validate_data(html_path)
    
    if extracted_data:
        load_data_to_postgresql(extracted_data)
        conditional_email_alert(extracted_data["aqi"])

if __name__ == "__main__":
    # To configure scheduling, use Prefect deployments:
    # prefect deploy --name aqi-etl --schedule "0 * * * *"
    aqi_etl_pipeline()