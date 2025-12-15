from datetime import timedelta
from typing import Dict

import pandas as pd

from dagster import (
    In,
    Nothing,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    job,
    op,
    schedule,
)


@op(
    out=Out(Dict, description="Metadata about the ingested CSV file"),
    retry_policy=RetryPolicy(max_retries=1, delay=timedelta(minutes=5)),
)
def ingest_csv() -> Dict:
    """
    Ingests a raw customer CSV file from a local path.
    Returns a dictionary containing the DataFrame and simple metadata.
    """
    # Placeholder path; replace with actual source location as needed.
    csv_path = "data/customers.csv"
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        # Return empty DataFrame if file is missing to keep pipeline runnable.
        df = pd.DataFrame()
    metadata = {
        "file_path": csv_path,
        "row_count": len(df),
        "columns": list(df.columns),
        "dataframe": df,
    }
    return metadata


@op(
    ins={"ingest": In(Dict)},
    out=Out(Dict, description="Quality score and flag indicating high quality"),
    retry_policy=RetryPolicy(max_retries=1, delay=timedelta(minutes=5)),
)
def quality_check(ingest: Dict) -> Dict:
    """
    Calculates a simple data quality score based on completeness.
    Returns a dict with the score and a boolean indicating whether the score meets the threshold.
    """
    df = ingest.get("dataframe", pd.DataFrame())
    if df.empty:
        score = 0.0
    else:
        completeness = df.notnull().mean().mean()  # proportion of non‑null cells
        # For illustration, treat completeness as the quality score (0‑100)
        score = round(completeness * 100, 2)
    high_quality = score >= 95.0
    return {"score": score, "high_quality": high_quality, "ingest": ingest}


@op(
    ins={"quality": In(Dict)},
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=1, delay=timedelta(minutes=5)),
)
def production_load(quality: Dict) -> Nothing:
    """
    Loads high‑quality data into the production database.
    This stub simply prints a message; replace with actual DB logic.
    """
    ingest = quality["ingest"]
    df = ingest.get("dataframe", pd.DataFrame())
    # Placeholder for DB load logic.
    print(f"[production_load] Loading {len(df)} rows to production database.")
    return Nothing


@op(
    ins={"quality": In(Dict)},
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=1, delay=timedelta(minutes=5)),
)
def quarantine_and_alert(quality: Dict) -> Nothing:
    """
    Moves low‑quality data to quarantine storage.
    This stub simply prints a message; replace with actual storage logic.
    """
    ingest = quality["ingest"]
    df = ingest.get("dataframe", pd.DataFrame())
    # Placeholder for quarantine logic.
    print(f"[quarantine_and_alert] Moving {len(df)} rows to quarantine storage.")
    return Nothing


@op(
    ins={"quality": In(Dict)},
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=1, delay=timedelta(minutes=5)),
)
def send_alert_email(quality: Dict) -> Nothing:
    """
    Sends an alert email to data stewards about low‑quality data.
    This stub simply prints a message; replace with actual email integration.
    """
    score = quality["score"]
    print(f"[send_alert_email] Alert: Data quality score {score}% is below threshold.")
    return Nothing


@op(
    ins={"quality": In(Dict)},
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=1, delay=timedelta(minutes=5)),
)
def cleanup(quality: Dict) -> Nothing:
    """
    Performs cleanup of temporary files or resources.
    """
    ingest = quality["ingest"]
    file_path = ingest.get("file_path", "unknown")
    print(f"[cleanup] Cleaning up resources for file {file_path}.")
    return Nothing


@job
def data_quality_gate_job():
    """
    Orchestrates the data quality gate pipeline:
    ingest_csv → quality_check → conditional branch → cleanup
    """
    ingest = ingest_csv()
    quality = quality_check(ingest)

    # Conditional branching based on quality score.
    if quality["high_quality"]:
        production_load(quality)
    else:
        quarantine_and_alert(quality)
        send_alert_email(quality)

    # Fan‑in: cleanup runs after whichever branch executed.
    cleanup(quality)


daily_schedule = ScheduleDefinition(
    job=data_quality_gate_job,
    cron_schedule="0 0 * * *",  # Every day at midnight UTC
    execution_timezone="UTC",
    description="Daily execution of the data quality gate pipeline.",
)


if __name__ == "__main__":
    result = data_quality_gate_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")