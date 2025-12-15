import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import smtplib
from bs4 import BeautifulSoup
from email.message import EmailMessage
from prefect import flow, task
from sqlalchemy import create_engine, text


@task
def fetch_html(url: str, html_path: Path) -> Path:
    """Download the AQI HTML page and save it locally."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(response.text, encoding="utf-8")
    return html_path


@task
def extract_and_validate(
    html_path: Path,
    json_path: Path,
    db_url: str,
) -> Optional[Dict[str, Any]]:
    """
    Parse the HTML, validate freshness, deduplicate against the DB,
    and write a structured JSON file.

    Returns the extracted data dictionary or ``None`` if the record is a duplicate.
    """
    # Load and parse HTML
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")

    # --- Extraction logic (simplified for illustration) ---
    # Assume the page contains a table with id="aqi-data" and rows:
    # <tr><td class="timestamp">2024-09-01 08:00</td>
    #     <td class="location">Bangkok</td>
    #     <td class="aqi">135</td></tr>
    table = soup.find("table", {"id": "aqi-data"})
    if not table:
        raise ValueError("AQI data table not found in HTML.")

    row = table.find("tr")
    if not row:
        raise ValueError("No data rows found in AQI table.")

    timestamp_str = row.find("td", {"class": "timestamp"}).get_text(strip=True)
    location = row.find("td", {"class": "location"}).get_text(strip=True)
    aqi_str = row.find("td", {"class": "aqi"}).get_text(strip=True)

    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
    aqi = int(aqi_str)

    data = {
        "timestamp": timestamp.isoformat(),
        "location": location,
        "aqi": aqi,
    }

    # --- Freshness check (optional) ---
    # For this example we accept any timestamp; real logic could compare to now.

    # --- Deduplication check ---
    engine = create_engine(db_url)
    with engine.connect() as conn:
        dup_query = text(
            """
            SELECT 1 FROM aqi_measurements
            WHERE timestamp = :ts AND location = :loc
            """
        )
        result = conn.execute(
            dup_query, {"ts": timestamp, "loc": location}
        ).fetchone()
        if result:
            # Record already exists; skip further processing.
            return None

    # Write JSON output
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return data


@task
def load_to_postgres(data: Dict[str, Any], db_url: str) -> None:
    """Insert the validated AQI data into PostgreSQL tables."""
    engine = create_engine(db_url)
    with engine.begin() as conn:
        insert_stmt = text(
            """
            INSERT INTO aqi_measurements (timestamp, location, aqi)
            VALUES (:ts, :loc, :aqi)
            """
        )
        conn.execute(
            insert_stmt,
            {
                "ts": datetime.fromisoformat(data["timestamp"]),
                "loc": data["location"],
                "aqi": data["aqi"],
            },
        )


@task
def conditional_email_alert(
    data: Dict[str, Any],
    email_config_path: Path,
) -> None:
    """
    Send an email alert if the AQI exceeds the configured threshold.
    The email configuration JSON should contain:
    {
        "recipients": ["example@example.com"],
        "threshold": 100,
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "username": "user@gmail.com",
        "password": "app-specific-password"
    }
    """
    if not email_config_path.is_file():
        raise FileNotFoundError(f"Email config not found: {email_config_path}")

    config = json.loads(email_config_path.read_text(encoding="utf-8"))
    threshold = config.get("threshold", 100)
    aqi = data.get("aqi")
    if aqi is None or aqi <= threshold:
        # AQI is within safe limits; no alert needed.
        return

    recipients: List[str] = config.get("recipients", [])
    if not recipients:
        raise ValueError("No email recipients configured.")

    subject = f"⚠️ Air Quality Alert – AQI {aqi} at {data['location']}"
    body = (
        f"The latest air quality measurement recorded on {data['timestamp']} "
        f"at {data['location']} has an AQI of {aqi}, which exceeds the safety "
        f"threshold of {threshold}.\n\nPlease take appropriate precautions."
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config["username"]
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(config["smtp_server"], config.get("smtp_port", 587)) as server:
        server.starttls()
        server.login(config["username"], config["password"])
        server.send_message(msg)


@flow
def mahidol_aqi_etl_flow(
    url: str = "https://aqi.mahidol.ac.th/report.html",
    html_path: str = "data/mahidol_aqi.html",
    json_path: str = "data/mahidol_aqi.json",
    db_url: str = os.getenv("POSTGRES_CONNECTION", "postgresql://user:pass@localhost:5432/aqi"),
    email_config_path: str = "config/email_config.json",
) -> None:
    """
    Orchestrates the Mahidol AQI ETL pipeline:
    1. Fetch HTML page.
    2. Extract, validate, and deduplicate data.
    3. Load data into PostgreSQL.
    4. Send email alert if AQI exceeds threshold.
    """
    html_file = Path(html_path)
    json_file = Path(json_path)
    email_cfg = Path(email_config_path)

    fetched_html = fetch_html(url, html_file)
    extracted_data = extract_and_validate(fetched_html, json_file, db_url)

    if extracted_data is None:
        # Duplicate record; pipeline ends here.
        return

    load_to_postgres(extracted_data, db_url)
    conditional_email_alert(extracted_data, email_cfg)


# Note: Scheduling for this flow should be configured via a Prefect deployment,
# e.g., using `prefect deployment create` with a cron schedule.

if __name__ == "__main__":
    mahidol_aqi_etl_flow()