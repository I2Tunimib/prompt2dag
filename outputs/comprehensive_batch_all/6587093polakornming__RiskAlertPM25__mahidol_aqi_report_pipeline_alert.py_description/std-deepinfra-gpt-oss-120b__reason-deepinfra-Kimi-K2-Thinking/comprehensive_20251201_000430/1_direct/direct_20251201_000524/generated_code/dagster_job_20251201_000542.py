import json
import os
import smtplib
import tempfile
from pathlib import Path
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup
from dagster import (
    Config,
    Field,
    InitResourceContext,
    InputDefinition,
    JobDefinition,
    OutputDefinition,
    ResourceDefinition,
    String,
    op,
    job,
    resource,
)


class FetchHtmlConfig(Config):
    url: str = "https://mahidol.ac.th/aqi-report"


@resource
def postgres_resource(init_context: InitResourceContext) -> Dict[str, str]:
    """Stub PostgreSQL resource."""
    return {
        "conn_str": "postgresql://user:password@localhost/air_quality",
    }


class EmailConfig(Config):
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    username: str = "example@gmail.com"
    password: str = "password"
    recipients: list[str] = ["alert@example.com"]
    threshold: int = 100  # AQI threshold for alerts


@resource
def email_resource(init_context: InitResourceContext) -> EmailConfig:
    """Email resource providing SMTP configuration."""
    return EmailConfig()


@op(
    config_schema=FetchHtmlConfig.to_config_schema(),
    out=OutputDefinition(str, description="Path to the saved HTML file"),
)
def fetch_html(context) -> str:
    """Download the AQI HTML page and store it locally."""
    url = context.op_config["url"]
    context.log.info(f"Fetching HTML from {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode="w", encoding="utf-8") as tmp_file:
        tmp_file.write(response.text)
        html_path = tmp_file.name

    context.log.info(f"Saved HTML to {html_path}")
    return html_path


@op(
    out=OutputDefinition(dict, description="Parsed AQI data as a dictionary"),
)
def extract_and_validate(context, html_path: str) -> Dict[str, Any]:
    """Parse the HTML, validate freshness, and deduplicate."""
    context.log.info(f"Parsing HTML from {html_path}")
    with open(html_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    # Example extraction logic – replace with real selectors as needed
    aqi_value_tag = soup.find("span", class_="aqi-value")
    if not aqi_value_tag:
        raise ValueError("AQI value not found in HTML")

    aqi_value = int(aqi_value_tag.text.strip())
    location_tag = soup.find("div", class_="location")
    location = location_tag.text.strip() if location_tag else "Unknown"

    data = {
        "aqi": aqi_value,
        "location": location,
        "source_url": context.op_config.get("url", "unknown"),
    }

    # Placeholder for freshness check – assume always fresh
    # Placeholder for duplicate check – assume not duplicate
    context.log.info(f"Extracted data: {data}")

    # Save JSON for debugging / downstream use
    json_path = Path(html_path).with_suffix(".json")
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(data, jf, ensure_ascii=False, indent=2)
    context.log.info(f"Saved extracted JSON to {json_path}")

    return data


@op(
    required_resource_keys={"postgres"},
    out=OutputDefinition(dict, description="Data passed through after loading"),
)
def load_to_postgres(context, data: Dict[str, Any]) -> Dict[str, Any]:
    """Load the AQI data into PostgreSQL tables."""
    conn_str = context.resources.postgres["conn_str"]
    context.log.info(f"Loading data into PostgreSQL using connection {conn_str}")

    # Stub implementation – replace with real DB logic (e.g., SQLAlchemy)
    # Example: INSERT INTO aqi_facts (location, aqi, timestamp) VALUES (...)
    context.log.info(f"Data to load: {data}")

    # Assume load succeeds
    context.log.info("Data loaded successfully")
    return data


@op(
    required_resource_keys={"email"},
    out=OutputDefinition(None, description="No output"),
)
def conditional_email_alert(context, data: Dict[str, Any]) -> None:
    """Send an email alert if AQI exceeds the configured threshold."""
    email_cfg: EmailConfig = context.resources.email
    aqi = data.get("aqi")
    if aqi is None:
        context.log.warning("AQI value missing; skipping email alert")
        return

    if aqi <= email_cfg.threshold:
        context.log.info(f"AQI {aqi} is below threshold {email_cfg.threshold}; no alert sent")
        return

    subject = f"Air Quality Alert: AQI {aqi}"
    body = f"The current AQI at {data.get('location', 'unknown')} is {aqi}, which exceeds the safe threshold of {email_cfg.threshold}."
    message = f"Subject: {subject}\n\n{body}"

    context.log.info(f"Sending alert email to {email_cfg.recipients}")
    try:
        with smtplib.SMTP(email_cfg.smtp_server, email_cfg.smtp_port) as server:
            server.starttls()
            server.login(email_cfg.username, email_cfg.password)
            server.sendmail(email_cfg.username, email_cfg.recipients, message)
        context.log.info("Alert email sent successfully")
    except Exception as exc:  # pylint: disable=broad-except
        context.log.error(f"Failed to send alert email: {exc}")


@job(
    resource_defs={
        "postgres": postgres_resource,
        "email": email_resource,
    }
)
def aqi_etl_job():
    """Dagster job orchestrating the AQI ETL pipeline."""
    html_path = fetch_html()
    data = extract_and_validate(html_path)
    loaded_data = load_to_postgres(data)
    conditional_email_alert(loaded_data)


if __name__ == "__main__":
    result = aqi_etl_job.execute_in_process()
    if result.success:
        print("AQI ETL job completed successfully")
    else:
        print("AQI ETL job failed")