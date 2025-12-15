import os
import json
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from prefect import flow, task
from sqlalchemy import create_engine, text


@task
def fetch_mahidol_aqi_html(
    url: str = "https://mahidol.ac.th/aqi-report",
    output_dir: str = "./data",
) -> Path:
    """Download the Mahidol AQI HTML page and save it locally."""
    os.makedirs(output_dir, exist_ok=True)
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    html_path = Path(output_dir) / "mahidol_aqi.html"
    html_path.write_text(response.text, encoding="utf-8")
    return html_path


@task
def extract_and_validate_data(
    html_path: Path,
    db_connection_string: str = os.getenv("POSTGRES_CONN", "postgresql://user:pass@localhost:5432/aqi"),
    mapping_path: str = "./config/pollution_mapping.json",
) -> Dict[str, Any]:
    """Parse the HTML, validate freshness, check duplicates, and return structured data."""
    # Load optional mapping (not used in this minimal example)
    if Path(mapping_path).exists():
        with open(mapping_path, "r", encoding="utf-8") as f:
            _ = json.load(f)  # placeholder for future use

    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")

    # Minimal parsing logic – replace with real selectors as needed
    aqi_value_tag = soup.find(class_="aqi-value")
    timestamp_tag = soup.find(class_="report-timestamp")
    location_tag = soup.find(class_="location")

    if not (aqi_value_tag and timestamp_tag and location_tag):
        raise ValueError("Required AQI data not found in HTML.")

    try:
        aqi = int(aqi_value_tag.get_text(strip=True))
    except ValueError as exc:
        raise ValueError("AQI value is not an integer.") from exc

    timestamp_str = timestamp_tag.get_text(strip=True)
    try:
        report_time = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("Timestamp format is invalid.") from exc

    # Freshness check: only process data from the last 24 hours
    now = datetime.now(timezone.utc)
    if now - report_time > timedelta(hours=24):
        raise ValueError("Report is older than 24 hours; skipping.")

    # Duplicate check against PostgreSQL
    engine = create_engine(db_connection_string)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM aqi_facts WHERE report_timestamp = :ts"
            ),
            {"ts": report_time},
        )
        count = result.scalar()
        if count and count > 0:
            raise ValueError("Duplicate report detected; skipping.")

    data = {
        "aqi": aqi,
        "report_timestamp": report_time.isoformat(),
        "location": location_tag.get_text(strip=True),
    }
    return data


@task
def load_data_to_postgresql(
    data: Dict[str, Any],
    db_connection_string: str = os.getenv("POSTGRES_CONN", "postgresql://user:pass@localhost:5432/aqi"),
) -> None:
    """Insert the validated AQI data into PostgreSQL tables."""
    engine = create_engine(db_connection_string)
    insert_stmt = text(
        """
        INSERT INTO aqi_facts (aqi, report_timestamp, location)
        VALUES (:aqi, :report_timestamp, :location)
        """
    )
    with engine.begin() as conn:
        conn.execute(
            insert_stmt,
            {
                "aqi": data["aqi"],
                "report_timestamp": data["report_timestamp"],
                "location": data["location"],
            },
        )


@task
def conditional_email_alert(
    data: Dict[str, Any],
    recipients_path: str = "./config/email_recipients.json",
    smtp_config_path: str = "./config/smtp_config.json",
    aqi_threshold: int = 100,
) -> None:
    """Send an email alert if AQI exceeds the safety threshold."""
    aqi = data.get("aqi")
    if aqi is None or aqi <= aqi_threshold:
        return  # No alert needed

    # Load recipients
    if not Path(recipients_path).exists():
        raise FileNotFoundError(f"Recipients file not found: {recipients_path}")
    with open(recipients_path, "r", encoding="utf-8") as f:
        recipients: List[str] = json.load(f)

    # Load SMTP configuration
    if not Path(smtp_config_path).exists():
        raise FileNotFoundError(f"SMTP config file not found: {smtp_config_path}")
    with open(smtp_config_path, "r", encoding="utf-8") as f:
        smtp_cfg: Dict[str, Any] = json.load(f)

    msg = EmailMessage()
    msg["Subject"] = f"Air Quality Alert: AQI {aqi}"
    msg["From"] = smtp_cfg["user"]
    msg["To"] = ", ".join(recipients)
    msg.set_content(
        f"""\
Alert!

The latest AQI reading is {aqi}, which exceeds the safety threshold of {aqi_threshold}.
Location: {data.get('location')}
Timestamp: {data.get('report_timestamp')}

Please take appropriate precautions.
"""
    )

    with smtplib.SMTP_SSL(smtp_cfg["host"], smtp_cfg.get("port", 465)) as server:
        server.login(smtp_cfg["user"], smtp_cfg["password"])
        server.send_message(msg)


@flow
def mahidol_aqi_etl_flow() -> None:
    """Orchestrate the Mahidol AQI ETL pipeline."""
    html_path = fetch_mahidol_aqi_html()
    data = extract_and_validate_data(html_path)
    load_data_to_postgresql(data)
    conditional_email_alert(data)


if __name__ == "__main__":
    # For local execution; in production, configure a Prefect deployment with a schedule.
    mahidol_aqi_etl_flow()