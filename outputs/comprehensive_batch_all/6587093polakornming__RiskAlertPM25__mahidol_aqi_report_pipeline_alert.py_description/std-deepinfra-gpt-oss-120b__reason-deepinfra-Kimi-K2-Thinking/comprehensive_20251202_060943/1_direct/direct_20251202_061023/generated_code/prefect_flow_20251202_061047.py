import os
import json
import logging
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from prefect import flow, task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task
def fetch_html(url: str, html_path: str) -> str:
    """Download the AQI HTML page and save it locally."""
    logger.info("Fetching HTML from %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    path = Path(html_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(response.text, encoding="utf-8")
    logger.info("Saved HTML to %s", path)
    return str(path)


@task
def extract_and_validate(
    html_path: str, mapping_path: str, db_url: str
) -> dict:
    """Parse the HTML, validate freshness, deduplicate, and return structured data."""
    logger.info("Extracting data from %s", html_path)
    html = Path(html_path).read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")

    # Example extraction logic – adapt to actual page structure
    data = {}
    table = soup.find("table", {"id": "aqi-table"})
    if not table:
        raise ValueError("AQI table not found in HTML")

    for row in table.find_all("tr"):
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) >= 2:
            metric, value = cols[0], cols[1]
            data[metric.lower().replace(" ", "_")] = value

    # Add timestamp
    data["timestamp"] = datetime.utcnow().isoformat()

    # Load pollution mapping (optional, for enrichment)
    mapping_file = Path(mapping_path)
    if mapping_file.is_file():
        mapping = json.loads(mapping_file.read_text(encoding="utf-8"))
        data["mapping"] = mapping

    # Freshness check – assume the page includes a date string
    # (placeholder logic; real implementation depends on source)
    if "date" in data:
        page_date = datetime.strptime(data["date"], "%Y-%m-%d")
        if (datetime.utcnow() - page_date).days > 1:
            raise ValueError("Data is not fresh")

    # Deduplication check against PostgreSQL
    engine = create_engine(db_url)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT 1 FROM aqi_facts WHERE timestamp = :ts LIMIT 1"
            ),
            {"ts": data["timestamp"]},
        )
        if result.fetchone():
            logger.info("Record for timestamp %s already exists – skipping.", data["timestamp"])
            raise RuntimeError("Duplicate data")

    logger.info("Extraction and validation successful")
    return data


@task
def load_to_postgres(data: dict, db_url: str) -> None:
    """Insert the structured data into PostgreSQL tables."""
    logger.info("Loading data into PostgreSQL")
    engine = create_engine(db_url)
    with engine.begin() as conn:
        # Example insertion – adapt to actual schema
        insert_stmt = text(
            """
            INSERT INTO aqi_facts (timestamp, aqi, pm25, pm10, no2, so2, o3, co)
            VALUES (:timestamp, :aqi, :pm25, :pm10, :no2, :so2, :o3, :co)
            """
        )
        conn.execute(
            insert_stmt,
            {
                "timestamp": data.get("timestamp"),
                "aqi": data.get("aqi"),
                "pm25": data.get("pm2.5"),
                "pm10": data.get("pm10"),
                "no2": data.get("no2"),
                "so2": data.get("so2"),
                "o3": data.get("o3"),
                "co": data.get("co"),
            },
        )
    logger.info("Data loaded successfully")


@task
def send_email_alert(data: dict, recipients_path: str, smtp_config: dict) -> None:
    """Send an email alert if AQI exceeds the safety threshold."""
    try:
        aqi_value = int(data.get("aqi", 0))
    except ValueError:
        logger.warning("Invalid AQI value: %s", data.get("aqi"))
        return

    threshold = 100  # Example safety threshold
    if aqi_value <= threshold:
        logger.info("AQI %s is within safe limits – no alert sent.", aqi_value)
        return

    logger.info("AQI %s exceeds threshold %s – sending alert.", aqi_value, threshold)

    # Load recipients
    recipients_file = Path(recipients_path)
    if not recipients_file.is_file():
        logger.error("Recipients file not found: %s", recipients_path)
        return
    recipients = json.loads(recipients_file.read_text(encoding="utf-8")).get("recipients", [])
    if not recipients:
        logger.error("No email recipients configured.")
        return

    # Build email
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["Subject"] = f"Air Quality Alert – AQI {aqi_value}"
    msg["From"] = smtp_config.get("user")
    msg["To"] = ", ".join(recipients)
    msg.set_content(
        f"The latest AQI reading is {aqi_value}, which exceeds the safety threshold of {threshold}.\n"
        f"Timestamp (UTC): {data.get('timestamp')}\n"
        "Please take appropriate precautions."
    )

    # Send via SMTP
    try:
        import smtplib

        with smtplib.SMTP(smtp_config["host"], smtp_config["port"]) as server:
            server.starttls()
            server.login(smtp_config["user"], smtp_config["password"])
            server.send_message(msg)
        logger.info("Alert email sent to %s", recipients)
    except Exception as exc:
        logger.exception("Failed to send alert email: %s", exc)


@flow
def mahidol_aqi_etl():
    """
    Prefect flow that orchestrates the Mahidol AQI ETL pipeline.
    """
    # Configuration
    url = "https://airquality.mahidol.ac.th/report.html"
    html_path = "data/mahidol_aqi.html"
    mapping_path = "config/pollution_mapping.json"
    recipients_path = "config/email_recipients.json"

    db_url = os.getenv(
        "POSTGRES_CONNECTION",
        "postgresql://user:password@localhost:5432/aqi",
    )
    smtp_config = {
        "host": os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "port": int(os.getenv("SMTP_PORT", "587")),
        "user": os.getenv("SMTP_USER"),
        "password": os.getenv("SMTP_PASSWORD"),
    }

    # Pipeline execution
    html_file = fetch_html(url, html_path)
    data = extract_and_validate(html_file, mapping_path, db_url)
    load_to_postgres(data, db_url)
    send_email_alert(data, recipients_path, smtp_config)


# Note: Scheduling for this flow should be configured in a Prefect deployment,
# e.g., using `prefect deployment create` with a cron schedule.

if __name__ == "__main__":
    mahidol_aqi_etl()