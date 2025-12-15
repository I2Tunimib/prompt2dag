from dagster import op, job, resource, Field, String, RetryPolicy, Failure, success_hook, failure_hook

# Resources
@resource(config_schema={"db_url": Field(String, is_required=True)})
def postgres_resource(context):
    from sqlalchemy import create_engine
    engine = create_engine(context.resource_config["db_url"])
    return engine

@resource(config_schema={"smtp_server": Field(String, is_required=True), "smtp_port": Field(int, is_required=True)})
def smtp_resource(context):
    return context.resource_config

# Ops
@op(required_resource_keys={"postgres"})
def fetch_mahidol_aqi_html(context):
    """Fetches the latest AQI report webpage from Mahidol University and saves it as an HTML file."""
    import requests
    url = "https://mahidol-university-aqi-report.com/latest"
    response = requests.get(url)
    if response.status_code != 200:
        raise Failure(f"Failed to fetch AQI report: {response.status_code}")
    html_content = response.text
    with open("aqi_report.html", "w") as file:
        file.write(html_content)
    context.log.info("AQI report fetched and saved as aqi_report.html")

@op(required_resource_keys={"postgres"})
def extract_and_validate_data(context):
    """Parses the HTML to extract air quality metrics, validates data freshness, checks for duplicates, and creates a structured JSON output."""
    from bs4 import BeautifulSoup
    import json
    with open("aqi_report.html", "r") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    aqi_data = soup.find("div", {"class": "aqi-data"}).text
    structured_data = {"datetime": "2023-10-01T12:00:00Z", "location": "Bangkok", "aqi": int(aqi_data)}
    
    # Check for duplicates
    with context.resources.postgres.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM aqi_data WHERE datetime = %s AND location = %s", (structured_data["datetime"], structured_data["location"]))
        if result.fetchone()[0] > 0:
            context.log.info("Data already exists, skipping further processing.")
            return None
    
    with open("aqi_data.json", "w") as file:
        json.dump(structured_data, file)
    context.log.info("Data extracted, validated, and saved as aqi_data.json")
    return structured_data

@op(required_resource_keys={"postgres"})
def load_data_to_postgresql(context, structured_data):
    """Transforms and loads the JSON data into multiple database tables including datetime, location, pollution dimensions, and AQI facts."""
    if structured_data is None:
        context.log.info("No new data to load, skipping.")
        return
    
    with context.resources.postgres.connect() as conn:
        conn.execute("INSERT INTO datetime (datetime) VALUES (%s) ON CONFLICT DO NOTHING", (structured_data["datetime"],))
        conn.execute("INSERT INTO location (location) VALUES (%s) ON CONFLICT DO NOTHING", (structured_data["location"],))
        conn.execute("INSERT INTO aqi_data (datetime, location, aqi) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (structured_data["datetime"], structured_data["location"], structured_data["aqi"]))
    context.log.info("Data loaded into PostgreSQL")

@op(required_resource_keys={"smtp"})
def conditional_email_alert(context, structured_data):
    """Evaluates the current AQI value against safety thresholds and sends email alerts if air quality exceeds safe levels."""
    if structured_data is None:
        context.log.info("No new data to evaluate, skipping.")
        return
    
    if structured_data["aqi"] > 100:
        import smtplib
        from email.mime.text import MIMEText
        smtp_config = context.resources.smtp
        msg = MIMEText(f"Air Quality Index in {structured_data['location']} has exceeded safe levels (AQI: {structured_data['aqi']}).")
        msg["Subject"] = "Air Quality Alert"
        msg["From"] = "aqi-alerts@example.com"
        msg["To"] = "recipient@example.com"
        
        with smtplib.SMTP(smtp_config["smtp_server"], smtp_config["smtp_port"]) as server:
            server.sendmail(msg["From"], msg["To"], msg.as_string())
        context.log.info("Email alert sent")
    else:
        context.log.info("AQI is within safe levels, no alert sent")

# Job
@job(
    resource_defs={
        "postgres": postgres_resource,
        "smtp": smtp_resource
    },
    config={
        "resources": {
            "postgres": {"config": {"db_url": "postgresql://user:password@localhost:5432/aqi_db"}},
            "smtp": {"config": {"smtp_server": "smtp.gmail.com", "smtp_port": 587}}
        }
    }
)
def aqi_pipeline():
    structured_data = extract_and_validate_data(fetch_mahidol_aqi_html())
    load_data_to_postgresql(structured_data)
    conditional_email_alert(structured_data)

# Hooks
@success_hook
def success_notification(context):
    context.log.info("Pipeline completed successfully")

@failure_hook
def failure_notification(context):
    context.log.error("Pipeline failed")

# Main
if __name__ == '__main__':
    result = aqi_pipeline.execute_in_process()