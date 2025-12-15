from pathlib import Path
import shutil
import smtplib
from email.message import EmailMessage
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=1, retry_delay_seconds=300)
def ingest_csv(source_path: str) -> Path:
    """
    Ingest raw customer CSV data from the given source location.

    Returns the path to the ingested file (copied to a temporary location).
    """
    source = Path(source_path)
    if not source.is_file():
        raise FileNotFoundError(f"Source CSV not found: {source}")

    temp_dir = Path("./temp_ingest")
    temp_dir.mkdir(parents=True, exist_ok=True)
    dest = temp_dir / source.name
    shutil.copy2(source, dest)
    return dest


@task(retries=1, retry_delay_seconds=300)
def quality_check(csv_path: Path) -> float:
    """
    Calculate a data quality score based on completeness and validity metrics.

    Returns a score between 0 and 100.
    """
    df = pd.read_csv(csv_path)

    # Simple completeness: proportion of non‑null cells
    completeness = df.notnull().mean().mean()

    # Simple validity: assume a column "email" should contain '@'
    if "email" in df.columns:
        validity = df["email"].dropna().apply(lambda x: "@" in str(x)).mean()
    else:
        validity = 1.0

    score = (completeness + validity) / 2 * 100
    return score


@task(retries=1, retry_delay_seconds=300)
def production_load(csv_path: Path) -> None:
    """
    Load high‑quality data into the production database.
    """
    engine = create_engine("sqlite:///production.db")  # Replace with real DB URL
    df = pd.read_csv(csv_path)
    df.to_sql("customers", con=engine, if_exists="append", index=False)


@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(csv_path: Path, quarantine_dir: str = "./quarantine") -> None:
    """
    Move low‑quality data to quarantine storage.
    """
    quarantine_path = Path(quarantine_dir)
    quarantine_path.mkdir(parents=True, exist_ok=True)
    dest = quarantine_path / csv_path.name
    shutil.move(str(csv_path), dest)


@task(retries=1, retry_delay_seconds=300)
def send_alert_email(
    subject: str = "Data Quality Alert",
    body: str = "Low‑quality data has been quarantined.",
    to_address: str = "data.steward@example.com",
    from_address: str = "no-reply@example.com",
    smtp_server: str = "localhost",
    smtp_port: int = 25,
) -> None:
    """
    Send an alert email to data stewards.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = to_address
    msg.set_content(body)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.send_message(msg)


@task(retries=1, retry_delay_seconds=300)
def cleanup(temp_dir: str = "./temp_ingest") -> None:
    """
    Perform final cleanup of temporary files and resources.
    """
    dir_path = Path(temp_dir)
    if dir_path.is_dir():
        shutil.rmtree(dir_path)


@flow
def data_quality_gate_flow(source_csv: str) -> None:
    """
    Orchestrates the data quality gate pipeline.

    Steps:
    1. Ingest CSV
    2. Quality check
    3. Conditional routing:
       - High quality → production load
       - Low quality → quarantine + alert email
    4. Cleanup
    """
    csv_path = ingest_csv(source_csv)
    score = quality_check(csv_path)

    # Branch based on quality score
    if score >= 95.0:
        # High‑quality path
        prod_future = production_load.submit(csv_path)
        prod_future.result()
    else:
        # Low‑quality path
        quarantine_future = quarantine_and_alert.submit(csv_path)
        quarantine_future.result()
        alert_future = send_alert_email.submit()
        alert_future.result()

    # Final cleanup (runs after whichever branch completes)
    cleanup.submit()


# Note: In a real deployment, configure a daily schedule via Prefect deployment YAML or UI.

if __name__ == "__main__":
    # Example local execution; replace with actual CSV path.
    data_quality_gate_flow(source_csv="data/customers.csv")