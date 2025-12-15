from dagster import op, job, resource, Field, execute_in_process
import requests
from bs4 import BeautifulSoup
import json
import psycopg2
import smtplib
from email.mime.text import MIMEText


@resource(config_schema={"db_config": Field(dict)})
def postgres_resource(context):
    config = context.resource_config["db_config"]
    conn = psycopg2.connect(**config)
    return conn


@resource(config_schema={"email_config": Field(dict)})
def email_resource(context):
    config = context.resource_config["email_config"]
    return config


@op(required_resource_keys={"postgres"})
def fetch_mahidol_aqi_html(context):
    url = "https://mahidol-university-aqi-report.com/latest"
    response = requests.get(url)
    response.raise_for_status()
    html_content = response.text
    with open("aqi_report.html", "w") as file:
        file.write(html_content)
    return "aqi_report.html"


@op(required_resource_keys={"postgres"})
def extract_and_validate_data(context, html_file):
    with open(html_file, "r") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    aqi_data = soup.find("div", {"class": "aqi-data"}).text
    aqi_json = json.loads(aqi_data)

    # Validate data freshness and deduplication
    conn = context.resources.postgres
    cursor = conn.cursor()
    cursor.execute("SELECT aqi_value FROM aqi_facts WHERE aqi_value = %s", (aqi_json["aqi_value"],))
    if cursor.fetchone():
        context.log.info("Data already exists, skipping further processing.")
        return None

    return aqi_json


@op(required_resource_keys={"postgres"})
def load_data_to_postgresql(context, aqi_json):
    if not aqi_json:
        context.log.info("No new data to load, skipping.")
        return

    conn = context.resources.postgres
    cursor = conn.cursor()

    # Load datetime
    cursor.execute(
        "INSERT INTO datetime (datetime) VALUES (%s) ON CONFLICT DO NOTHING",
        (aqi_json["datetime"],)
    )

    # Load location
    cursor.execute(
        "INSERT INTO location (location_name) VALUES (%s) ON CONFLICT DO NOTHING",
        (aqi_json["location"],)
    )

    # Load pollution dimensions
    for pollutant, value in aqi_json["pollutants"].items():
        cursor.execute(
            "INSERT INTO pollution (pollutant, value) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (pollutant, value)
        )

    # Load AQI facts
    cursor.execute(
        "INSERT INTO aqi_facts (datetime_id, location_id, aqi_value) VALUES (%s, %s, %s)",
        (aqi_json["datetime_id"], aqi_json["location_id"], aqi_json["aqi_value"])
    )

    conn.commit()


@op(required_resource_keys={"email"})
def conditional_email_alert(context, aqi_json):
    if not aqi_json:
        context.log.info("No new data to evaluate, skipping.")
        return

    if aqi_json["aqi_value"] > 100:
        email_config = context.resources.email
        msg = MIMEText(f"High AQI detected: {aqi_json['aqi_value']}")
        msg["Subject"] = "Air Quality Alert"
        msg["From"] = email_config["sender"]
        msg["To"] = ", ".join(email_config["recipients"])

        with smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"]) as server:
            server.starttls()
            server.login(email_config["sender"], email_config["password"])
            server.sendmail(email_config["sender"], email_config["recipients"], msg.as_string())


@job(resource_defs={"postgres": postgres_resource, "email": email_resource})
def aqi_pipeline():
    html_file = fetch_mahidol_aqi_html()
    aqi_json = extract_and_validate_data(html_file)
    load_data_to_postgresql(aqi_json)
    conditional_email_alert(aqi_json)


if __name__ == "__main__":
    result = aqi_pipeline.execute_in_process()