<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let me start by understanding the steps involved.

First, the pipeline has four main steps: fetching HTML, extracting and validating data, loading to PostgreSQL, and sending conditional email alerts. Each of these will be a Dagster op. The dependencies are linear, so each op depends on the previous one.

For the Fetch Mahidol AQI HTML op, I'll use the requests library to download the HTML. I'll need to handle exceptions here, maybe log if something goes wrong. The output is the HTML content, which will be passed to the next op.

Next, the Extract and Validate Data op. This uses BeautifulSoup to parse the HTML. I'll need to extract the necessary metrics, check data freshness, and check for duplicates. Since the original mentions checking against the database, maybe this op needs a database resource. But since the user wants simplified resources, I can add a comment about a PostgreSQL resource. The output here is a structured JSON, maybe a dictionary with the extracted data.

Then, the Load Data to PostgreSQL op. This takes the JSON data and inserts it into the database. Again, using a PostgreSQL resource, perhaps with a connection. I'll need to structure the data into different tables as mentioned. Maybe using SQLAlchemy or psycopg2, but for simplicity, I'll just outline the steps with comments.

The Conditional Email Alert op checks the AQI against thresholds. If it's above a certain level, send an email via SMTP. The email configuration (like sender, recipients, password) can be handled through config. I'll use smtplib for this. The op should only send the email if the condition is met, so there's conditional logic inside the op.

Now, resources. The user mentioned PostgreSQL and SMTP. I'll create a PostgreSQL resource stub as a comment. For the email, maybe use a configurable resource or pass the settings via op config.

The job will tie all these ops together in order. Since dependencies are linear, each op's output is the input to the next.

Testing the code structure: each op is decorated with @op, the job with @job. The main block runs execute_in_process.

Wait, the original pipeline uses Airflow's PostgresHook, but in Dagster, resources are used instead. So I need to define a postgres_resource, maybe using a connection object. But the user said to provide minimal stubs, so I can just comment that part.

Also, the email configuration needs to be handled. The op can take a config_schema with the necessary parameters like smtp_server, sender_email, etc. The password could be set via config, but in practice, it's better to use secrets. For simplicity, I'll include them in the config.

Let me structure the code step by step:

1. Import necessary modules: dagster's op, job, resource, etc. Also requests, BeautifulSoup, json, smtplib.

2. Define the ops with appropriate inputs and outputs.

3. Set up the job with the ops in order.

4. Add the main block to execute the job.

Check for PEP8 compliance, function names, and docstrings.

Possible issues: The database operations are stubbed, but the code should still run without errors. For the email, the config needs to be provided when running the job. The user might need to fill in their own SMTP details.

I think that covers the main points. Now, putting it all together in code.
</think>

from dagster import op, job, resource, Config
import requests
from bs4 import BeautifulSoup
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime

# --- Resources ---
@resource(config_schema={"conn_string": str})
def postgres_resource(context):
    return context.resource_config["conn_string"]

# --- Config Classes ---
class EmailConfig(Config):
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str
    sender_password: str
    recipients: list

# --- Ops ---
@op
def fetch_mahidol_aqi_html():
    """Fetches latest AQI HTML report from Mahidol University website"""
    url = "https://www.mahidol.ac.th/aqi"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch AQI data: {str(e)}")

@op
def extract_and_validate_data(context, html_content: str):
    """Extracts air quality metrics from HTML and validates data freshness"""
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Simplified extraction - implement actual parsing logic here
    extracted_data = {
        "timestamp": datetime.now().isoformat(),
        "location": "Bangkok",
        "aqi": 45,
        "pm25": 12.3,
        "pm10": 24.6
    }
    
    context.log.info(f"Extracted data: {extracted_data}")
    
    # Implement actual duplicate check against database here
    if context.resources.postgres.check_duplicate(extracted_data["timestamp"]):
        raise Exception("Duplicate data detected - skipping processing")
    
    return extracted_data

@op(required_resource_keys={"postgres"})
def load_data_to_postgresql(context, data: dict):
    """Loads transformed data into PostgreSQL database"""
    # Implement actual database loading logic here
    context.log.info(f"Loading data to PostgreSQL: {data}")
    
    # Example table inserts
    queries = [
        f"INSERT INTO aqi_facts (timestamp, location, aqi) VALUES ('{data['timestamp']}', '{data['location']}', {data['aqi']})"
    ]
    
    try:
        conn = context.resources.postgres.get_connection()
        with conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Database operation failed: {str(e)}")

@op
def conditional_email_alert(context, data: dict, email_config: EmailConfig):
    """Sends email alerts if AQI exceeds safety thresholds"""
    aqi = data.get("aqi", 0)
    
    if aqi > 50:
        msg = EmailMessage()
        msg["Subject"] = f"Air Quality Alert: AQI {aqi}"
        msg["From"] = email_config.sender_email
        msg["To"] = ", ".join(email_config.recipients)
        msg.set_content(f"Warning: Air Quality Index reached {aqi} at {data['timestamp']}")
        
        try:
            with smtplib.SMTP(email_config.smtp_server, email_config.smtp_port) as server:
                server.starttls()
                server.login(email_config.sender_email, email_config.sender_password)
                server.send_message(msg)
            context.log.info("Alert email sent successfully")
        except Exception as e:
            context.log.error(f"Failed to send email alert: {str(e)}")
    else:
        context.log.info("AQI within safe limits - no alert needed")

# --- Job Definition ---
@job(resource_defs={"postgres": postgres_resource})
def aqi_etl_pipeline():
    html_content = fetch_mahidol_aqi_html()
    extracted_data = extract_and_validate_data(html_content)
    db_result = load_data_to_postgresql(extracted_data)
    conditional_email_alert(extracted_data)

# --- Execution ---
if __name__ == "__main__":
    # Example configuration
    run_config = {
        "resources": {
            "postgres": {"config": {"conn_string": "postgresql://user:pass@localhost/db"}}
        },
        "ops": {
            "conditional_email_alert": {
                "config": {
                    "sender_email": "user@gmail.com",
                    "sender_password": "app_password",
                    "recipients": ["alert@example.com"]
                }
            }
        }
    }
    
    result = aqi_etl_pipeline.execute_in_process(run_config=run_config)