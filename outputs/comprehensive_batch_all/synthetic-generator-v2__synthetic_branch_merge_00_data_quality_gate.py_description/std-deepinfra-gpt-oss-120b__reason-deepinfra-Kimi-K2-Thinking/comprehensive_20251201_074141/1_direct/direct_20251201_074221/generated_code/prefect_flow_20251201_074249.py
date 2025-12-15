import pathlib
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import datetime
import shutil
import time


@task(retries=1, retry_delay_seconds=300)
def ingest_csv(source_path: str = "data/customers.csv") -> dict:
    """
    Ingest raw customer CSV data from the local file system.

    Returns a dictionary containing the DataFrame and metadata such as
    the file path and ingestion timestamp.
    """
    file_path = pathlib.Path(source_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Source CSV not found at {file_path}")

    df = pd.read_csv(file_path)
    metadata = {
        "file_path": file_path,
        "ingested_at": datetime.utcnow(),
        "dataframe": df,
    }
    return metadata


@task(retries=1, retry_delay_seconds=300)
def quality_check(metadata: dict) -> float:
    """
    Calculate a simple data quality score based on completeness.

    Returns a percentage score between 0 and 100.
    """
    df: pd.DataFrame = metadata["dataframe"]
    total_cells = df.shape[0] * df.shape[1]
    non_null_cells = df.count().sum()
    completeness = non_null_cells / total_cells if total_cells else 0

    # Placeholder for additional validity metrics
    validity = 1.0

    score = (completeness * 0.6 + validity * 0.4) * 100
    return round(score, 2)


@task(retries=1, retry_delay_seconds=300)
def production_load(metadata: dict, score: float) -> None:
    """
    Load high‑quality data into the production database.

    This task is a no‑op when the quality score is below the threshold.
    """
    if score < 95.0:
        return  # Skip loading for low‑quality data

    # Placeholder for actual DB load logic
    df: pd.DataFrame = metadata["dataframe"]
    print(f"[production_load] Loading {len(df)} records to production database.")


@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(metadata: dict, score: float) -> None:
    """
    Move low‑quality data to quarantine storage.

    This task is a no‑op when the quality score meets the threshold.
    """
    if score >= 95.0:
        return  # Skip quarantine for high‑quality data

    source_path: pathlib.Path = metadata["file_path"]
    quarantine_dir = pathlib.Path("quarantine")
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    destination = quarantine_dir / source_path.name
    shutil.move(str(source_path), str(destination))
    print(f"[quarantine_and_alert] Moved file to quarantine: {destination}")


@task(retries=1, retry_delay_seconds=300)
def send_alert_email(metadata: dict) -> None:
    """
    Send an alert email to data stewards about low‑quality data.
    """
    # Placeholder for email integration
    print("[send_alert_email] Alert sent to data stewards regarding low‑quality data.")


@task(retries=1, retry_delay_seconds=300)
def cleanup(metadata: dict) -> None:
    """
    Perform final cleanup of temporary resources.
    """
    # Example cleanup: remove any temporary files if they exist
    temp_dir = pathlib.Path("temp")
    if temp_dir.is_dir():
        shutil.rmtree(temp_dir)
        print("[cleanup] Temporary directory removed.")
    print("[cleanup] Cleanup completed.")


@flow
def data_quality_gate_flow():
    """
    Orchestrates the data quality gate pipeline:
    ingest → quality check → conditional branch (production or quarantine) →
    optional alert → cleanup.
    """
    # Ingest CSV
    metadata = ingest_csv()

    # Compute quality score
    score = quality_check(metadata)

    # Fan‑out to both possible branches (tasks decide internally whether to act)
    prod_future = production_load.submit(metadata, score)
    quar_future = quarantine_and_alert.submit(metadata, score)

    # If data is low quality, wait for quarantine then send alert
    if score < 95.0:
        quar_future.result()
        alert_future = send_alert_email.submit(metadata)
        alert_future.result()
    else:
        prod_future.result()

    # Final cleanup (runs after whichever path completed)
    cleanup(metadata)


if __name__ == "__main__":
    # This flow is intended to run daily via a Prefect deployment.
    # Deployment configuration (e.g., schedule) should be defined separately.
    data_quality_gate_flow()