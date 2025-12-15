<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by breaking down the steps mentioned. The pipeline has four main steps: fetching HTML, extracting and validating data, loading into PostgreSQL, and conditionally sending emails. Each of these will be Dagster ops.

First, the Fetch Mahidol AQI HTML step. This op should download the HTML from the specified URL. I'll use the requests library for that. I'll add a config schema for the URL so it's configurable. The output will be the HTML content, which I'll pass to the next op.

Next, the Extract and Validate Data op. This needs to parse the HTML using BeautifulSoup. I'll need to extract the necessary data, validate freshness, check for duplicates. Since the problem mentions deduplication checks against the database, but in a real scenario, that would require a database connection. For simplicity, maybe I'll just check if the data is not empty or some basic validation. The output here is structured JSON data.

Then, the Load Data to PostgreSQL op. This op takes the JSON data and loads it into the database. Since the user mentioned using PostgresHook in Airflow, but in Dagster, we'd use a resource. I'll create a minimal PostgreSQL resource stub. The op will depend on this resource. The actual loading logic would involve inserting into tables, but for the code, I'll just print a message or log it.

Conditional Email Alert op. This op checks the AQI value against a threshold. If it's above, send an email. The email configuration (like SMTP settings and recipients) can be handled via config. Again, for the sake of the example, maybe just print instead of actually sending emails. The op should only run if the previous steps succeeded, which in Dagster's linear dependencies is automatic.

Now, dependencies are linear, so each op depends on the previous one. So the job will define the execution order as fetch_html -> extract_data -> load_data -> send_alert.

For resources, like PostgreSQL and email, I'll define them as resources. The PostgreSQL resource can be a simple class with a connection method. The email resource could hold SMTP configuration. But since the user said to provide simplified examples as comments or stubs, maybe just include them as commented config or minimal classes.

Configurations: Each op might need some config, like the URL for fetching, thresholds for alerts, email recipients, etc. I'll use Dagster's config schema for those.

Skip logic: The problem mentions tasks may skip execution based on conditions. In Dagster, ops can't be skipped unless they're not executed. So perhaps within the ops, we have conditional checks. For example, in the extract_data op, if data is not fresh or duplicate, it might return None, but the next ops would need to handle that. Alternatively, using conditional execution with Dagster's DynamicOutput or something, but since the example is linear, maybe just have the ops return early if conditions aren't met.

Testing the code: Make sure all ops are connected, and the job is defined with the right dependencies. Also, include the main block to execute in process.

Potential issues: Handling the database connection and email sending in a minimal way without actual implementations. So stubbing those parts with print statements or logs.

Putting it all together, each op is decorated with @op, the job with @job, and resources are provided. Config is added where necessary. Let me structure the code step by step.
</think>

from dagster import op, job, resource, Field, String, Dict, List
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import smtplib

# --- Resources ---
@resource(config_schema={
    "host": String,
    "dbname": String,
    "user": String,
    "password": String
})
def postgres_resource(context):
    class PostgreSQLConnection:
        def __init__(self, config):
            self.config = config
        
        def execute_query(self, query):
            # Stub implementation
            print(f"Executing query: {query}")
    return PostgreSQLConnection(context.resource_config)

@resource(config_schema={
    "smtp_server": String,
    "smtp_port": int,
    "sender_email": String,
    "sender_password": String
})
def email_resource(context):
    return context.resource_config

# --- Ops ---
@op(config_schema={"url": Field(String, default_value="https://aqmthai.com/report.php")})
def fetch_mahidol_aqi_html(context):
    url = context.op_config["url"]
    response = requests.get(url)
    response.raise_for_status()
    return response.text

@op
def extract_and_validate_data(context, html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Example extraction - implement actual parsing logic
    data = {
        "timestamp": datetime.now().isoformat(),
        "location": "Bangkok",
        "aqi": 45,
        "pollutants": {"pm25": 12.3}
    }
    
    # Simplified validation
    if not data.get("aqi"):
        raise ValueError("Invalid data extracted")
    
    context.log.info("Data validation successful")
    return json.dumps(data)

@op(required_resource_keys={"postgres"})
def load_data_to_postgresql(context, json_data):
    data = json.loads(json_data)
    
    # Example loading logic
    queries = [
        f"INSERT INTO aqi_facts (timestamp, location, aqi) VALUES ('{data['timestamp']}', '{data['location']}', {data['aqi']})"
    ]
    
    for query in queries:
        context.resources.postgres.execute_query(query)
    context.log.info("Data loaded successfully")

@op(config_schema={
    "threshold": Field(int, default_value=50),
    "recipients": Field(List[str], default_value=["admin@example.com"])
})
def conditional_email_alert(context, json_data):
    data = json.loads(json_data)
    aqi = data["aqi"]
    
    if aqi > context.op_config["threshold"]:
        email_cfg = context.resources.email_config
        msg = f"Subject: AQI Alert\n\nAQI level {aqi} exceeds safety threshold!"
        
        # Stub implementation
        context.log.info(f"Would send email to {context.op_config['recipients']}:\n{msg}")
    else:
        context.log.info("AQI within safe limits - no alert needed")

# --- Job ---
@job(resource_defs={
    "postgres": postgres_resource,
    "email_config": email_resource
})
def air_quality_etl_job():
    html = fetch_mahidol_aqi_html()
    json_data = extract_and_validate_data(html)
    load_result = load_data_to_postgresql(json_data)
    conditional_email_alert(json_data)

if __name__ == "__main__":
    # Example configuration
    run_config = {
        "resources": {
            "postgres": {
                "config": {
                    "host": "localhost",
                    "dbname": "aqi",
                    "user": "postgres",
                    "password": "postgres"
                }
            },
            "email_config": {
                "config": {
                    "smtp_server": "smtp.gmail.com",
                    "smtp_port": 587,
                    "sender_email": "alerts@example.com",
                    "sender_password": "password"
                }
            }
        }
    }
    air_quality_etl_job.execute_in_process(run_config=run_config)