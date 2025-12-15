from dagster import (
    op,
    graph,
    job,
    RetryPolicy,
    ScheduleDefinition,
    get_dagster_logger,
)


def _default_retry_policy() -> RetryPolicy:
    """Common retry policy: 1 retry with a 5‑minute delay."""
    return RetryPolicy(max_retries=1, delay=300)


@op(
    config_schema={"path": str},
    retry_policy=_default_retry_policy(),
    description="Ingest raw customer CSV data and return file metadata.",
)
def ingest_csv(context) -> dict:
    path = context.op_config["path"]
    logger = get_dagster_logger()
    logger.info(f"Ingesting CSV from {path}")
    # Minimal stub: just return metadata; real implementation would read the file.
    return {"path": path, "size_bytes": 0}


@op(
    retry_policy=_default_retry_policy(),
    description="Calculate data quality score and decide routing.",
)
def quality_check(context, csv_meta: dict) -> dict:
    logger = get_dagster_logger()
    # Placeholder quality calculation.
    # In a real scenario, you would compute completeness and validity metrics.
    if "good" in csv_meta["path"]:
        score = 98.0
    else:
        score = 85.0
    is_high_quality = score >= 95.0
    logger.info(
        f"Quality score for {csv_meta['path']}: {score}% – "
        f"{'high' if is_high_quality else 'low'} quality"
    )
    return {"score": score, "is_high_quality": is_high_quality, "csv_meta": csv_meta}


@op(
    retry_policy=_default_retry_policy(),
    description="Load high‑quality data into the production database.",
)
def production_load(context, csv_meta: dict) -> None:
    logger = get_dagster_logger()
    logger.info(f"Loading {csv_meta['path']} into production database")
    # Stub: replace with actual DB load logic.


@op(
    retry_policy=_default_retry_policy(),
    description="Move low‑quality data to quarantine storage.",
)
def quarantine_and_alert(context, csv_meta: dict) -> None:
    logger = get_dagster_logger()
    logger.info(f"Moving {csv_meta['path']} to quarantine storage")
    # Stub: replace with actual file move logic.


@op(
    retry_policy=_default_retry_policy(),
    description="Send an alert email to data stewards about quarantined data.",
)
def send_alert_email(context) -> None:
    logger = get_dagster_logger()
    logger.info("Sending alert email to data stewards")
    # Stub: replace with actual email sending logic.


@op(
    retry_policy=_default_retry_policy(),
    description="Perform final cleanup of temporary files and resources.",
)
def cleanup(context) -> None:
    logger = get_dagster_logger()
    logger.info("Running cleanup of temporary resources")
    # Stub: replace with actual cleanup logic.


@graph
def quality_gate():
    """Orchestrates the data quality gate pipeline."""
    csv_meta = ingest_csv()
    qc_result = quality_check(csv_meta)

    if qc_result["is_high_quality"]:
        production_load(csv_meta)
    else:
        quarantine_and_alert(csv_meta)
        send_alert_email()

    cleanup()


quality_gate_job = quality_gate.to_job(
    name="quality_gate_job",
    description="Daily data quality gate job with branching and cleanup.",
)


daily_quality_gate_schedule = ScheduleDefinition(
    job=quality_gate_job,
    cron_schedule="0 0 * * *",  # Every day at midnight UTC
    execution_timezone="UTC",
    description="Runs the quality_gate_job once per day.",
)


if __name__ == "__main__":
    # Example execution with a sample CSV path.
    result = quality_gate_job.execute_in_process(
        run_config={
            "ops": {
                "ingest_csv": {"config": {"path": "data/customers_good.csv"}}
            }
        }
    )
    assert result.success