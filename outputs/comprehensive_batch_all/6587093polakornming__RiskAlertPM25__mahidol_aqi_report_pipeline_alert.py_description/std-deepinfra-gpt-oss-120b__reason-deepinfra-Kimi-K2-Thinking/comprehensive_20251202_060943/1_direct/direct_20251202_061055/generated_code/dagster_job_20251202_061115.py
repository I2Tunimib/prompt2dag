import os
import json
import datetime
import smtplib
from email.message import EmailMessage

import requests
from bs4 import BeautifulSoup

from dagster import op, job, ResourceDefinition, ConfigurableResource, InitResourceContext, OpExecutionContext, String, Bool, Float, List, Field, In, Out, Nothing, Dict, StringSource


class PostgresResource(ConfigurableResource):
    """Minimal PostgreSQL resource stub."""

    connection_string: str = Field(
        default="postgresql://user:password@localhost:5432/air_quality",
        description="SQLAlchemy connection string for PostgreSQL.",
    )

    def get_connection(self):
        """Return a placeholder connection object."""
        # In a real implementation you would return an engine or connection.
        # Here we simply return the connection string for demonstration.
        return self.connection_string


class EmailResource(ConfigurableResource):
    """Minimal SMTP email resource stub."""

    smtp_server: str = Field(default="smtp.gmail.com", description="SMTP server host.")
    smtp_port: int = Field(default=587, description="SMTP server port.")
    username: str = Field(default="example@gmail.com", description="SMTP username.")
    password: str = Field(default="password", description="SMTP password.")

    def send_email(self, subject: str, body: str, recipients: list[str]) -> None:
        """Send an email using the configured SMTP server."""
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.username
        msg["To"] = ", ".join(recipients)
        msg.set_content(body)

        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.starttls()
            server.login(self.username, self.password)
            server.send_message(msg)


@op(
    config_schema={
        "url": String,
        "output_path": String,
    },
    out=Out(String),
    description="Downloads the latest AQI HTML report and saves it locally.",
)
def fetch_mahidol_aqi_html(context: OpExecutionContext) -> str:
    url = context.op_config["url"]
    output_path = context.op_config["output_path"]
    context.log.info(f"Fetching AQI HTML from {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(response.text)

    context.log.info(f"Saved HTML to {output_path}")
    return output_path


@op(
    config_schema={
        "duplicate_check": Bool,
    },
    ins={"html_path": In(String)},
    out=Out(Dict),
    description="Parses HTML, validates freshness, checks duplicates, and returns structured JSON.",
)
def extract_and_validate_data(context: OpExecutionContext, html_path: str) -> dict:
    duplicate_check = context.op_config["duplicate_check"]
    context.log.info(f"Reading HTML from {html_path}")

    with open(html_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    # Example parsing logic – adapt to actual page structure.
    # Here we assume the page contains a table with id="aqi-table".
    table = soup.find("table", {"id": "aqi-table"})
    if not table:
        raise ValueError("AQI table not found in HTML.")

    data = {}
    for row in table.find_all("tr")[1:]:  # Skip header row
        cols = row.find_all("td")
        if len(cols) < 3:
            continue
        location = cols[0].get_text(strip=True)
        aqi_value = int(cols[1].get_text(strip=True))
        timestamp_str = cols[2].get_text(strip=True)
        timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
        data[location] = {
            "aqi": aqi_value,
            "timestamp": timestamp.isoformat(),
        }

    # Freshness check – ensure at least one record is from today.
    today = datetime.datetime.utcnow().date()
    if not any(
        datetime.datetime.fromisoformat(entry["timestamp"]).date() == today
        for entry in data.values()
    ):
        raise ValueError("No fresh data for today found in the report.")

    # Duplicate check – placeholder logic.
    if duplicate_check:
        # In a real implementation you would query the DB.
        # Here we assume no duplicates.
        context.log.info("Duplicate check enabled – assuming no duplicates.")
    else:
        context.log.info("Duplicate check disabled.")

    json_path = os.path.splitext(html_path)[0] + ".json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    context.log.info(f"Extracted data saved to {json_path}")
    return data


@op(
    required_resource_keys={"postgres"},
    description="Loads the structured JSON data into PostgreSQL tables.",
)
def load_data_to_postgres(context: OpExecutionContext, data: dict) -> Nothing:
    postgres: PostgresResource = context.resources.postgres
    conn_str = postgres.get_connection()
    context.log.info(f"Connecting to PostgreSQL with {conn_str}")

    # Placeholder for actual DB insertion logic.
    # In production you would use SQLAlchemy or psycopg2.
    for location, metrics in data.items():
        context.log.info(
            f"Inserting record for {location}: AQI={metrics['aqi']}, timestamp={metrics['timestamp']}"
        )
    context.log.info("Data load completed.")
    return Nothing


@op(
    required_resource_keys={"email"},
    config_schema={
        "aqi_threshold": Float,
        "recipients": List(String),
    },
    description="Sends email alerts if any AQI exceeds the configured threshold.",
)
def conditional_email_alert(context: OpExecutionContext, data: dict) -> Nothing:
    email: EmailResource = context.resources.email
    threshold = context.op_config["aqi_threshold"]
    recipients = context.op_config["recipients"]

    alerts = [
        (loc, metrics["aqi"])
        for loc, metrics in data.items()
        if metrics["aqi"] > threshold
    ]

    if not alerts:
        context.log.info("All AQI values are within safe limits; no email sent.")
        return Nothing

    subject = "Air Quality Alert – AQI Exceeded Threshold"
    body_lines = ["The following locations have AQI values above the threshold:\n"]
    for loc, aqi in alerts:
        body_lines.append(f"- {loc}: AQI {aqi}")
    body = "\n".join(body_lines)

    context.log.info(f"Sending alert email to {recipients}")
    email.send_email(subject=subject, body=body, recipients=recipients)
    context.log.info("Alert email sent.")
    return Nothing


@job(
    resource_defs={
        "postgres": ResourceDefinition.hardcoded_resource(PostgresResource()),
        "email": ResourceDefinition.hardcoded_resource(EmailResource()),
    },
)
def mahidol_aqi_etl_job():
    html_path = fetch_mahidol_aqi_html()
    data = extract_and_validate_data(html_path)
    load_data_to_postgres(data)
    conditional_email_alert(data)


if __name__ == "__main__":
    result = mahidol_aqi_etl_job.execute_in_process(
        run_config={
            "ops": {
                "fetch_mahidol_aqi_html": {
                    "config": {
                        "url": "https://example.com/mahidol-aqi.html",
                        "output_path": "data/mahidol_aqi.html",
                    }
                },
                "extract_and_validate_data": {"config": {"duplicate_check": False}},
                "conditional_email_alert": {
                    "config": {
                        "aqi_threshold": 100.0,
                        "recipients": ["alert_recipient@example.com"],
                    }
                },
            }
        }
    )
    assert result.success