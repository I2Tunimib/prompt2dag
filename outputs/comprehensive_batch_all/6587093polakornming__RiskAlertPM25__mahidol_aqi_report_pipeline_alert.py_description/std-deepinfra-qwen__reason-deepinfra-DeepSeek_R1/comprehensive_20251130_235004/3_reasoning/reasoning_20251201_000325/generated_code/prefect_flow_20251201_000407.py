<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the steps and constraints.

First, the pipeline has four steps: fetch HTML, extract and validate data, load to PostgreSQL, and conditional email alert. The steps are sequential with no branching or parallelism, so each task depends on the previous one.

The constraints mention using Prefect 2.x APIs, so I'll import flow and task decorators. Each step should be a @task, and the main @flow will orchestrate them in order. Since there's no parallelism, I don't need to use .submit() here; just call the tasks normally.

For each task:

1. Fetch Mahidol AQI HTML: Use requests to get the webpage and save as HTML. Need to handle exceptions maybe, but the problem says to avoid placeholders, so I'll include basic code with a try-except block.

2. Extract and Validate Data: Parse HTML with BeautifulSoup, extract metrics, check data freshness, check duplicates. Since the database check is mentioned, I'll assume a function like check_duplicate_in_db exists. Maybe load a config JSON for pollution mapping.

3. Load Data to PostgreSQL: Transform JSON into tables and insert into PostgreSQL. Use psycopg2 for connection, but the user might have a PostgresHook. Since the original used Airflow's PostgresHook, but Prefect might use a different method. For simplicity, I'll use psycopg2 directly, but note that in a real scenario, they might use Prefect's SQLAlchemy or another block.

4. Conditional Email Alert: Check AQI against thresholds, send email via SMTP. Use smtplib. Config for recipients from a JSON file.

Now, considering dependencies: Each task is called in sequence, so the flow function will call them one after another, passing outputs where needed. For example, the HTML content from fetch is passed to extract, which produces JSON data for load, and the AQI value from extract is used in the alert.

I need to include necessary imports: requests, bs4, json, smtplib, psycopg2 (or similar), os, datetime. Also, from prefect import flow, task.

The tasks should have retries or other Prefect features if needed, but the problem doesn't specify, so maybe just basic tasks.

For the email and database connections, I'll use context managers where appropriate. Also, the config files are external, so I'll add functions to load them, like load_pollution_mapping and load_email_config.

Skip logic: The extract task checks for duplicates and freshness. If data is not fresh or is a duplicate, maybe return None, and subsequent tasks can check if they should run. But the problem says tasks include conditional checks that may skip execution. So in the extract task, if it's a duplicate, perhaps it raises a prefect.Abort or returns a signal. Alternatively, the task could return a flag, and the flow decides to skip. However, since tasks are strictly linear, maybe the tasks themselves handle the skipping internally.

But Prefect tasks don't have built-in skip unless using conditional logic. So perhaps within each task, if the condition isn't met, the task does nothing and returns None or a flag. The next task would need to handle that. Alternatively, use Prefect's skip mechanism. But the user's code example might not include that, so perhaps handle it within the task logic.

For example, in the extract task, after checking duplicates, if it's a duplicate, the task could log a message and return None. Then the load task would check if the data is None and skip processing. Similarly, the email alert task checks if AQI is above threshold.

But the problem says "tasks include conditional checks that may skip execution", so the tasks themselves have logic to decide whether to perform their action. So in code, inside each task function, there might be an if statement that skips the main work if conditions aren't met.

Now, putting it all together:

- The flow function calls each task in order, passing the required parameters.
- Each task is decorated with @task.
- The main guard is if __name__ == '__main__': which calls the flow.

Potential issues: Handling database connections and email configuration. For simplicity, I'll write functions that load from JSON files, but in reality, these might be Prefect blocks or managed configurations.

Also, the original uses Airflow's PostgresHook, but in Prefect, we might use a different approach. Since the user wants executable code, I'll use psycopg2 to connect, assuming the user has the necessary environment variables (like DB credentials).

Testing for data freshness: The extract task checks if the data is recent. Maybe compare the timestamp in the HTML to the current time.

Error handling: Each task might have try-except blocks, but the problem doesn't specify, so minimal handling.

Now, writing the code step by step.

Imports:

from prefect import flow, task
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import smtplib
from email.message import EmailMessage
import psycopg2
import os

Then, the tasks:

@task
def fetch_mahidol_aqi_html():
    # URL from Mahidol's site
    url = "http://example.com/aqi-report"  # Placeholder URL
    try:
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.text
        # Save to file (optional, but step mentions saving)
        with open("aqi_report.html", "w") as f:
            f.write(html_content)
        return html_content
    except Exception as e:
        print(f"Failed to fetch HTML: {e}")
        raise

@task
def extract_and_validate_data(html_content):
    # Load pollution mapping config
    pollution_mapping = load_pollution_mapping()
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    # Extract data - example extraction, actual selectors may vary
    # Assuming data is in a table or specific elements
    # Placeholder extraction:
    aqi_value = int(soup.find('div', class_='aqi-value').text)
    timestamp_str = soup.find('span', class_='timestamp').text
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    
    # Check data freshness (e.g., within last hour)
    current_time = datetime.now()
    if (current_time - timestamp).total_seconds() > 3600:
        print("Data is not fresh, skipping processing")
        return None
    
    # Check duplicates against DB
    if check_duplicate_in_db(timestamp):
        print("Duplicate data, skipping processing")
        return None
    
    # Create structured JSON
    data = {
        "timestamp": timestamp.isoformat(),
        "aqi": aqi_value,
        "pollutants": {}  # Assume extracted from HTML
    }
    with open("aqi_data.json", "w") as f:
        json.dump(data, f)
    return data

def load_pollution_mapping():
    # Load from config file
    with open("pollution_mapping.json") as f:
        return json.load(f)

def check_duplicate_in_db(timestamp):
    # Connect to DB and check if timestamp exists
    # Placeholder: return False
    return False

@task
def load_data_to_postgresql(data):
    if not data:
        print("No data to load")
        return
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST")
    )
    try:
        with conn.cursor() as cur:
            # Insert into datetime, location, pollution, aqi tables
            # Example: insert timestamp into datetime table
            cur.execute(
                "INSERT INTO datetime (timestamp) VALUES (%s) ON CONFLICT DO NOTHING",
                (data["timestamp"],)
            )
            # Similar for other tables
            conn.commit()
    finally:
        conn.close()

@task
def conditional_email_alert(data):
    if not data:
        print("No data for alert check")
        return
    aqi = data.get("aqi")
    if aqi is None:
        return
    # Load email config
    email_config = load_email_config()
    threshold = email_config.get("threshold", 100)
    if aqi > threshold:
        send_alert_email(aqi, email_config)

def load_email_config():
    with open("email_config.json") as f:
        return json.load(f)

def send_alert_email(aqi, config):
    msg = EmailMessage()
    msg.set_content(f"AQI level {aqi} exceeds safety threshold!")
    msg["Subject"] = "AQI Alert"
    msg["From"] = config["sender"]
    msg["To"] = ", ".join(config["recipients"])
    
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(config["sender"], config["password"])
        server.send_message(msg)

@flow
def aqi_etl_pipeline():
    html_content = fetch_mahidol_aqi_html()
    extracted_data = extract_and_validate_data(html_content)
    load_data_to_postgresql(extracted_data)
    conditional_email_alert(extracted_data)

if __name__ == "__main__":
    aqi_etl_pipeline()

Wait, but in Prefect, tasks are called within the flow. So each task is invoked, and their outputs are passed. The code seems okay.

Potential issues:

- The functions load_pollution_mapping, check_duplicate_in_db, load_email_config, send_alert_email are not @tasks, which is correct because they are helper functions.

- The extract task returns None if data is not fresh or duplicate, which causes subsequent tasks to skip processing.

- The load_data_to_postgresql task checks if data is None and skips.

- The email task also checks data and AQI.

- The database connection uses environment variables, which the user must set.

- The email config includes a password, which in real life should be a Prefect Secret, but here it's loaded from a JSON file for simplicity.

- The code assumes that the HTML structure is known, with specific classes like 'aqi-value' and 'timestamp', which might need adjustment based on the actual website.

- The pollution mapping and email config files are expected to be in the same directory.

This should meet the requirements: tasks are sequential, each step is a @task, flow orchestrates them, code is executable with necessary imports, and the main guard is present.
</think>

from prefect import flow, task
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import smtplib
from email.message import EmailMessage
import psycopg2
import os

@task
def fetch_mahidol_aqi_html():
    url = "https://aqmthai.com/report.php"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open("aqi_report.html", "w") as f:
            f.write(response.text)
        return response.text
    except Exception as e:
        raise Exception(f"Failed to fetch HTML: {str(e)}")

@task
def extract_and_validate_data(html_content):
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        data_table = soup.find("table", {"id": "aqi_table"})
        
        with open("pollution_mapping.json") as f:
            pollution_map = json.load(f)
        
        extracted_data = {
            "timestamp": datetime.now().isoformat(),
            "measurements": {},
            "location": "Bangkok"
        }
        
        for row in data_table.find_all("tr")[1:]:
            cells = row.find_all("td")
            pollutant = cells[0].text.strip()
            value = float(cells[1].text)
            extracted_data["measurements"][pollution_map.get(pollutant)] = value
        
        if check_duplicate(extracted_data["timestamp"]):
            return None
            
        with open("aqi_data.json", "w") as f:
            json.dump(extracted_data, f)
            
        return extracted_data
    except Exception as e:
        raise Exception(f"Data extraction failed: {str(e)}")

def check_duplicate(timestamp):
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST")
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM aqi_data WHERE timestamp = %s)", (timestamp,))
            return cur.fetchone()[0]
    finally:
        conn.close()

@task
def load_data_to_postgresql(data):
    if not data:
        return
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
        )
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO aqi_data (timestamp, location, measurements)
                VALUES (%s, %s, %s)
            """, (data["timestamp"], data["location"], json.dumps(data["measurements"])))
            conn.commit()
    except Exception as e:
        raise Exception(f"Database load failed: {str(e)}")
    finally:
        conn.close()

@task
def conditional_email_alert(data):
    if not data:
        return
        
    with open("email_config.json") as f:
        email_config = json.load(f)
    
    max_aqi = max(data["measurements"].values(), default=0)
    if max_aqi > email_config["threshold"]:
        msg = EmailMessage()
        msg.set_content(f"AQI Alert: {max_aqi} exceeds safety threshold!")
        msg["Subject"] = "Air Quality Alert"
        msg["From"] = email_config["sender"]
        msg["To"] = ", ".join(email_config["recipients"])
        
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(email_config["sender"], email_config["app_password"])
                server.send_message(msg)
        except Exception as e:
            raise Exception(f"Email alert failed: {str(e)}")

@flow
def aqi_etl_pipeline():
    html_content = fetch_mahidol_aqi_html()
    extracted_data = extract_and_validate_data(html_content)
    load_data_to_postgresql(extracted_data)
    conditional_email_alert(extracted_data)

if __name__ == "__main__":
    aqi_etl_pipeline()