<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the steps involved. The pipeline has four main steps: fetching HTML, extracting data, loading to PostgreSQL, and sending conditional email alerts. Each step depends on the previous one, so the dependencies are linear.

First, I'll create an op for each step. The ops will be decorated with @op. The job will tie them together in order. Let's name the ops fetch_mahidol_aqi_html, extract_and_validate_data, load_data_to_postgresql, and conditional_email_alert.

For the fetch step, I'll use requests to get the HTML content and save it. Need to handle exceptions maybe, but the user said minimal, so perhaps just a basic request. The output is the HTML content.

Next, the extraction op. It takes the HTML, parses with BeautifulSoup, extracts metrics, checks data freshness, duplicates. The output is a structured JSON. But how to check duplicates against the database? Since resources are needed, maybe create a PostgreSQL resource. But the user mentioned to provide simplified resources as comments or stubs. So I'll add a PostgresResource class as a stub, maybe with a connection handle. The extract op will need to access the database to check duplicates, so it should use the resource. So the op will have a required resource.

Then the load data op. It takes the JSON data and loads into PostgreSQL. Again, using the PostgresResource. This op depends on the extract step's output.

The conditional email alert op checks the AQI from the extracted data. If above threshold, send email. It uses an SMTP resource. So another resource for SMTP, maybe with config like host, port, username, password. The op will require this resource.

Now, the job needs to include these ops with dependencies. Since execution is linear, each op depends on the previous. So in the job definition, the order is fetch -> extract -> load -> email.

Resources configuration: The user said to provide minimal stubs. So in the job, I can configure the resources with placeholders. For example, postgres_resource and smtp_resource are configured with dummy values. The resources are passed into the job via with_resources.

Testing the main block: The example uses execute_in_process when running as __main__.

Wait, the original pipeline uses Airflow's PostgresHook, but in Dagster, we should abstract that into a resource. So the PostgresResource will handle the connection. Similarly for SMTP.

Also, the extract step needs to validate data freshness and check duplicates. For simplicity, maybe the function just checks if the data exists (dummy check here) and skips if needed. But the code needs to be executable, so I'll add a minimal check, perhaps a comment indicating where the actual duplicate check would be.

The email alert op needs to have conditional logic. So inside the op, after getting the AQI value, if it exceeds the threshold, send the email using the SMTP resource.

Now, putting it all together. Each op is defined with the necessary inputs and context. Resources are set up as classes and provided to the job.

I should also include the necessary imports: dagster's op, job, resource, etc., plus requests, BeautifulSoup, smtplib, json, datetime.

Wait, the user said to avoid placeholders. So any unknown parts should have minimal safe defaults. For example, in the extract op, the duplicate check could be a placeholder function that returns False, meaning no duplicate. Similarly, the data extraction from HTML would need example parsing, but since the actual structure isn't provided, maybe simulate extracting some data.

Testing for code validity: The code should run without syntax errors. So functions should return appropriate outputs even if stubbed.

So, the code outline would be:

- Define resources (PostgresResource, SMTPResource) as stubs.
- Define ops with inputs and context.
- Set up the job with dependencies and resources.
- Include the main block.

Let me draft the code step by step.

Imports: from dagster import op, job, resource, get_dagster_logger, Output, Out. Also import requests, BeautifulSoup, smtplib, etc.

PostgresResource: a class with a handle method returning a connection stub.

SMTPResource: similar, with a client method returning an SMTP client.

Fetch op: uses requests.get, saves HTML, returns content.

Extract op: takes html_content, uses BeautifulSoup to parse, extracts data, checks duplicates using postgres resource. If duplicate, skip. Outputs the JSON data.

Load op: takes the data, uses postgres to insert into tables.

Email alert: takes data, checks AQI, uses smtp resource to send email if needed.

Job: linear dependencies, with resources configured.

Main block: execute_in_process.

Now, code:

Implement the ops with proper inputs and context. For example, the extract op's context has resources.postgres.

In the extract op, after parsing, create a data dict. Then check for duplicates. If exists, skip. Else proceed. But how to handle skipping? Maybe return an output with a flag. Or use Dagster's conditional outputs. But since the user said linear execution, perhaps within the op, if data is duplicate, skip processing. But the next ops still need to run. Hmm, but the original pipeline says tasks may skip execution if data exists. So maybe the extract op could conditionally output None or a flag, and downstream ops check that. But the user's note says to model branching within ops or via fan-in. Since the original is linear with possible skips, perhaps the ops have internal logic to skip processing.

Alternatively, if the data is duplicate, the extract op doesn't produce output, but the job's dependencies require it. That might not work. So perhaps the extract op always returns the data, but the load op checks if it's a duplicate and skips insertion. Or the extract op returns a flag indicating whether it's new data. Then the load op can decide to proceed or not.

But the original description says "skip logic: tasks include conditional checks that may skip execution if data already exists". So each task has its own skip logic. So in the extract op, if data is duplicate, it skips processing (i.e., does not produce new data), but how does the next op know to proceed? Maybe the extract op returns None, and the load op handles that. But in Dagster, if an op returns None, the next op's input is None. So the load op would need to handle that.

But the user's pipeline says "strict linear dependency where each task requires successful completion of the previous task". So even if data is duplicate, the tasks still run but skip their processing. So perhaps each op has internal logic to check if they should proceed.

For example, the extract op checks for duplicates. If duplicate, it logs a message and returns the data anyway (or a flag), but the load op would check if the data is new before inserting. Alternatively, the extract op may decide not to return data, but that would break the linear dependency. So maybe the data is passed along, but the load op checks if it's a duplicate again.

Alternatively, the extract op adds a 'is_duplicate' flag to the output. The load op checks this flag and skips insertion if it's a duplicate. But the original description says "built-in deduplication checks to prevent processing redundant data", so maybe the extract step checks and skips processing if duplicate. But how does that affect downstream steps? If the data is duplicate, maybe the transformation and load steps are skipped. But in a linear pipeline, all steps must execute. So perhaps each step includes their own checks.

This is getting complicated. To keep it simple and linear, perhaps each op performs its own checks. For example, the extract op checks for data freshness and duplicates. If it's a duplicate, it still returns the data but marks it as a duplicate. The load op then checks if it's a duplicate and skips the database insertion. Similarly, the email alert checks the AQI regardless.

But the user's note says "if something is unknown, add minimal, safe defaults or comments". So perhaps in the extract op, the duplicate check is stubbed as a check that always returns False (no duplicate), so the data is processed. Similarly, the load op proceeds to insert.

In code:

For the extract op:

def extract_and_validate_data(context, html_content):
    logger = get_dagster_logger()
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    # Extract data - example implementation
    data = {
        'timestamp': datetime.datetime.now().isoformat(),
        'location': 'Bangkok',
        'aqi': 45,
        'pollutants': {'PM2.5': 12}
    }
    # Check data freshness (stub)
    is_fresh = True  # Actual implementation would check timestamp
    # Check duplicates using PostgreSQL
    with context.resources.postgres.get_conn() as conn:
        # Example duplicate check query
        # cursor = conn.cursor()
        # cursor.execute("SELECT EXISTS(...)")
        # is_duplicate = cursor.fetchone()[0]
        is_duplicate = False  # Stub for example
    if not is_fresh or is_duplicate:
        logger.info("Skipping processing: data not fresh or duplicate")
        return None  # Or some indication to skip
    return data

But if the extract op returns None, the load op would receive None. So in the load op:

@op(required_resource_keys={"postgres"})
def load_data_to_postgresql(context, data):
    if data is None:
        context.log.info("No data to load")
        return
    # Proceed to load data into PostgreSQL
    # Example implementation
    context.log.info(f"Loading data into database: {data}")
    # Insert into tables...

Similarly, the email alert op would check if data exists:

@op(required_resource_keys={"smtp"})
def conditional_email_alert(context, data):
    if data is None:
        context.log.info("No data for email alert")
        return
    aqi = data.get('aqi', 0)
    if aqi > 50:  # Example threshold
        # Send email
        subject = "AQI Alert!"
        body = f"AQI level {aqi} exceeds safety threshold"
        with context.resources.smtp.get_client() as smtp_client:
            smtp_client.sendmail(
                from_addr="sender@example.com",
                to_addrs=["recipient@example.com"],
                msg=f"Subject: {subject}\n\n{body}"
            )
        context.log.info("Sent email alert")
    else:
        context.log.info("AQI within safe limits")

But this approach requires that each downstream op handles the case where data is None. However, according to the original pipeline, each task requires successful completion of the previous task. So even if data is skipped, the tasks still run but may do nothing. So this approach works.

But in Dagster, if an op returns None, the next op's input is None. So the code must handle that.

Putting it all together, the code would have the four ops, each taking the previous output, and handling None if needed.

Now, the resources:

@resource(config_schema={"connection_string": str})
def postgres_resource(context):
    class PostgresConnection:
        def get_conn(self):
            # Stub connection
            return "PostgresConnectionStub"
    return PostgresConnection()

Similarly for SMTP:

@resource(config_schema={"host": str, "port": int, "username": str, "password": str})
def smtp_resource(context):
    class SMTPClient:
        def __init__(self, config):
            self.config = config
        def sendmail(self, from_addr, to_addrs, msg):
            # Stub sendmail
            print(f"Email sent from {from_addr} to {to_addrs}")
    class SMTPWrapper:
        def __init__(self, context):
            self.config = context.resource_config
        def get_client(self):
            return SMTPClient(self.config)
    return SMTPWrapper(context)

Wait, perhaps the SMTP resource should return a client that can be used in a 'with' block, like smtplib.SMTP. But for simplicity, the stub just logs.

But in the email alert op, the code uses 'with context.resources.smtp.get_client() as smtp_client', which implies that the resource's get_client() returns a context manager. Alternatively, the SMTP resource could directly provide the client.

Alternatively, for the sake of the example, the SMTP resource's get_client returns a dummy object with a sendmail method.

In any case, the resources are stubbed with minimal implementations.

Finally, the job:

@job(
    resource_defs={
        "postgres": postgres_resource,
        "smtp": smtp_resource
    }
)
def aqi_etl_job():
    html_content = fetch_mahidol_aqi_html()
    data = extract_and_validate_data(html_content)
    load_result = load_data_to_postgresql(data)
    conditional_email_alert(data)

Wait, the dependencies are linear: each op depends on the previous. So the job should chain them correctly.

In the job function, each op is called in sequence, passing the output of the previous as input to the next.

So:

def aqi_etl_job():
    data = extract_and_validate_data(fetch_mahidol_aqi_html())
    load_data_to_postgresql(data)
    conditional_email_alert(data)

But in Dagster, the job function defines the dependencies by invoking the ops in order and passing their outputs. So the above code correctly sets up the dependencies.

Now, the main block:

if __name__ == "__main__":
    result = aqi_etl_job.execute_in_process(run_config={
        "resources": {
            "postgres": {"config": {"connection_string": "postgresql://user:pass@localhost/db"}},
            "smtp": {"config": {"host": "smtp.gmail.com", "port": 587, "username": "user", "password": "pass"}}
        }
    })

Putting all together, the code should be executable, with stubbed resources and example data extraction.

Testing for PEP 8 compliance: function names are snake_case, proper spacing, etc.

I think that covers all the requirements. Now, write the code accordingly.
</think>

from dagster import op, job, resource, get_dagster_logger
import requests
from bs4 import BeautifulSoup
import smtplib
import json
from datetime import datetime

logger = get_dagster_logger()

@resource(config_schema={"connection_string": str})
def postgres_resource(context):
    class PostgresConnection:
        def get_conn(self):
            logger.info("Connecting to PostgreSQL")
            return "CONNECTION_STUB"
    return PostgresConnection()

@resource(config_schema={"host": str, "port": int, "username": str, "password": str})
def smtp_resource(context):
    class SMTPClient:
        def __init__(self, config):
            self.config = config
        
        def sendmail(self, from_addr, to_addrs, msg):
            logger.info(f"Simulating email to {to_addrs}: {msg[:50]}...")
    return SMTPClient(context.resource_config)

@op
def fetch_mahidol_aqi_html():
    url = "http://air4thai.pcd.go.th/webV2/aqiInfo.php"
    response = requests.get(url)
    response.raise_for_status()
    return response.text

@op(required_resource_keys={"postgres"})
def extract_and_validate_data(context, html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Example data extraction - implement actual parsing logic here
    extracted_data = {
        "timestamp": datetime.now().isoformat(),
        "location": "Bangkok",
        "aqi": 65,
        "pollutants": {"PM2.5": 35.4}
    }
    
    # Duplicate check stub
    with context.resources.postgres.get_conn() as conn:
        logger.info("Checking for duplicates...")
        is_duplicate = False
    
    if is_duplicate:
        logger.info("Skipping duplicate data")
        return None
    return extracted_data

@op(required_resource_keys={"postgres"})
def load_data_to_postgresql(context, data):
    if not data:
        logger.info("No data to load")
        return
    
    logger.info(f"Loading data: {json.dumps(data, indent=2)}")
    with context.resources.postgres.get_conn() as conn:
        logger.info("Inserting into database tables...")

@op(required_resource_keys={"smtp"})
def conditional_email_alert(context, data):
    if not data:
        logger.info("No data for alert check")
        return
    
    aqi_threshold = 50
    if data["aqi"] > aqi_threshold:
        smtp_client = context.resources.smtp
        recipients = ["admin@example.com"]
        message = f"Subject: AQI Alert!\n\nAQI level {data['aqi']} exceeds {aqi_threshold}"
        
        smtp_client.sendmail(
            from_addr="alerts@example.com",
            to_addrs=recipients,
            msg=message
        )
        logger.info("Alert email sent")
    else:
        logger.info("AQI within safe limits")

@job(
    resource_defs={
        "postgres": postgres_resource,
        "smtp": smtp_resource
    }
)
def aqi_etl_job():
    html = fetch_mahidol_aqi_html()
    data = extract_and_validate_data(html)
    load_data_to_postgresql(data)
    conditional_email_alert(data)

if __name__ == "__main__":
    run_config = {
        "resources": {
            "postgres": {
                "config": {"connection_string": "postgresql://user:pass@localhost/test"}
            },
            "smtp": {
                "config": {
                    "host": "smtp.gmail.com",
                    "port": 587,
                    "username": "user",
                    "password": "pass"
                }
            }
        }
    }
    result = aqi_etl_job.execute_in_process(run_config=run_config)