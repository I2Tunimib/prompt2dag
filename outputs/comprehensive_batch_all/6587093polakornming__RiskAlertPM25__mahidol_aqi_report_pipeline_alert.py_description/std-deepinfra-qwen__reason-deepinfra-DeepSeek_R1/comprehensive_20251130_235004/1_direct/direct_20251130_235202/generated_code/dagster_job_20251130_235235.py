from dagster import op, job, resource, RetryPolicy, Failure, Field, String, get_dagster_logger
import requests
from bs4 import BeautifulSoup
import json
import psycopg2
import smtplib
from email.mime.text import MIMEText

logger = get_dagster_logger()

@resource(config_schema={"host": Field(String), "user": Field(String), "password": Field(String), "dbname": Field(String)})
def postgres_resource(context):
    conn = psycopg2.connect(
        host=context.resource_config["host"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        dbname=context.resource_config["dbname"]
    )
    return conn

@resource(config_schema={"smtp_server": Field(String), "smtp_port": Field(int), "email_user": Field(String), "email_password": Field(String)})
def smtp_resource(context):
    return {
        "smtp_server": context.resource_config["smtp_server"],
        "smtp_port": context.resource_config["smtp_port"],
        "email_user": context.resource_config["email_user"],
        "email_password": context.resource_config["email_password"]
    }

@op(required_resource_keys={"postgres"})
def fetch_mahidol_aqi_html(context):
    url = "https://mahidol-university-aqi-report.com/latest"
    response = requests.get(url)
    response.raise_for_status()
    html_content = response.text
    with open("aqi_report.html", "w") as file:
        file.write(html_content)
    logger.info("Fetched and saved AQI HTML report.")
    return "aqi_report.html"

@op(required_resource_keys={"postgres"})
def extract_and_validate_data(context, html_file):
    with open(html_file, "r") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    aqi_data = {
        "datetime": soup.find("div", {"class": "datetime"}).text,
        "location": soup.find("div", {"class": "location"}).text,
        "aqi_value": int(soup.find("div", {"class": "aqi-value"}).text)
    }
    
    conn = context.resources.postgres
    cursor = conn.cursor()
    cursor.execute("SELECT aqi_value FROM aqi_facts WHERE datetime = %s AND location = %s", (aqi_data["datetime"], aqi_data["location"]))
    existing_data = cursor.fetchone()
    
    if existing_data:
        logger.info("Data already exists in the database. Skipping further processing.")
        return None
    
    with open("aqi_data.json", "w") as file:
        json.dump(aqi_data, file)
    logger.info("Extracted and validated AQI data.")
    return "aqi_data.json"

@op(required_resource_keys={"postgres"})
def load_data_to_postgresql(context, json_file):
    if not json_file:
        logger.info("No new data to load. Skipping database load.")
        return
    
    with open(json_file, "r") as file:
        aqi_data = json.load(file)
    
    conn = context.resources.postgres
    cursor = conn.cursor()
    
    cursor.execute("INSERT INTO datetime (datetime) VALUES (%s) ON CONFLICT DO NOTHING", (aqi_data["datetime"],))
    cursor.execute("INSERT INTO location (location) VALUES (%s) ON CONFLICT DO NOTHING", (aqi_data["location"],))
    cursor.execute("INSERT INTO pollution (aqi_value) VALUES (%s) ON CONFLICT DO NOTHING", (aqi_data["aqi_value"],))
    cursor.execute("INSERT INTO aqi_facts (datetime, location, aqi_value) VALUES (%s, %s, %s)", (aqi_data["datetime"], aqi_data["location"], aqi_data["aqi_value"]))
    
    conn.commit()
    logger.info("Loaded AQI data into PostgreSQL database.")

@op(required_resource_keys={"smtp"})
def conditional_email_alert(context, json_file):
    if not json_file:
        logger.info("No new data to evaluate. Skipping email alert.")
        return
    
    with open(json_file, "r") as file:
        aqi_data = json.load(file)
    
    if aqi_data["aqi_value"] > 100:
        smtp_config = context.resources.smtp
        msg = MIMEText(f"Air Quality Index (AQI) has exceeded safe levels: {aqi_data['aqi_value']}")
        msg["Subject"] = "Air Quality Alert"
        msg["From"] = smtp_config["email_user"]
        msg["To"] = "recipient@example.com"
        
        with smtplib.SMTP(smtp_config["smtp_server"], smtp_config["smtp_port"]) as server:
            server.starttls()
            server.login(smtp_config["email_user"], smtp_config["email_password"])
            server.sendmail(smtp_config["email_user"], ["recipient@example.com"], msg.as_string())
        
        logger.info("Sent email alert for high AQI value.")
    else:
        logger.info("AQI value is within safe levels. No alert sent.")

@job(resource_defs={"postgres": postgres_resource, "smtp": smtp_resource})
def aqi_pipeline():
    html_file = fetch_mahidol_aqi_html()
    json_file = extract_and_validate_data(html_file)
    load_data_to_postgresql(json_file)
    conditional_email_alert(json_file)

if __name__ == "__main__":
    result = aqi_pipeline.execute_in_process()