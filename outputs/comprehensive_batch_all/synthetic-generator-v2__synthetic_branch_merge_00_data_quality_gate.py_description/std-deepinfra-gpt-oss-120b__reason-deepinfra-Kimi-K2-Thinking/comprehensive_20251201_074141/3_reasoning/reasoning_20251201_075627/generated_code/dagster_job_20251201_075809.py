from dagster import (
    op,
    job,
    RetryPolicy,
    ScheduleDefinition,
    Definitions,
    OpExecutionContext,
)
import os
import shutil
from typing import Dict, Any


def calculate_quality_score(file_path: str) -> float:
    """Stub for quality calculation - in production, implement real completeness/validity checks."""
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            # Simple heuristic for demo: more complete lines = higher score
            if len(lines) <= 1:
                return 0.0
            return min(100.0, 50.0 + len(lines) * 0.5)
    except Exception:
        return 0.0


def load_to_production_db(file_path: str) -> None:
    """Stub for production database load - in production, implement real DB insertion."""
    print(f"Loading {file_path} to production database")


def move_to_quarantine_storage(file_path: str) -> str:
    """Stub for quarantine - in production, implement secure quarantine move."""
    quarantine_dir = "/tmp/quarantine"
    os.makedirs(quarantine_dir, exist_ok=True)
    filename = os.path.basename(file_path)
    quarantine_path = os.path.join(quarantine_dir, filename)
    shutil.move(file_path, quarantine_path)
    return quarantine_path


def send_quality_alert_email(quality_score: float, quarantine_path: str) -> None:
    """Stub for email alert - in production, implement SMTP email sending."""
    print(f"ALERT: Data quality {quality_score}% - File quarantined: {quarantine_path}")


def cleanup_temp_resources(file_path: str) -> None:
    """Stub for cleanup - removes temporary files."""
    if os.path.exists(file_path):
        os.remove(file_path)


@op(
    description="Ingests raw customer CSV data from source location and returns file metadata",
    config_schema={"source_file_path": str},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def ingest_csv(context: OpExecutionContext) -> Dict[str, Any]:
    source_file_path = context.op_config["source_file_path"]
    if not os.path.exists(source_file_path):
        raise FileNotFoundError(f"Source file not found: {source_file_path}")
    
    record_count = sum(1 for _ in open(source_file_path)) - 1
    context.log.info(f"Ingested CSV: {source_file_path} with {record_count} records")
    return {"file_path": source_file_path, "record_count": record_count}


@op(
    description="Calculates data quality score and determines routing path",
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def quality_check(context: OpExecutionContext, file_metadata: Dict[str, Any]) -> Dict[str, Any]:
    file_path = file_metadata["file_path"]
    quality_score = calculate_quality_score(file_path)
    high_quality = quality_score >= 95.0
    
    context.log.info(
        f"Quality check: score={quality_score:.1f}%, high_quality={high_quality}"
    )
    return {
        "file_path": file_path,
        "record_count": file_metadata["record_count"],
        "quality_score": quality_score,
        "high_quality": high_quality,
    }


@op(
    description="Loads high-quality data to production database",
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def production_load(context: OpExecutionContext, quality_result: Dict[str, Any]) -> Dict[str, Any]:
    if not quality_result["high_quality"]:
        context.log.info("Skipping production load: quality below threshold")
        return {"status": "skipped"}
    
    file_path = quality_result["file_path"]
    context.log.info(f"Loading to production: {file_path}")
    load_to_production_db(file_path)
    return {"status": "loaded", "records": quality_result["record_count"]}


@op(
    description="Moves low-quality data to quarantine storage",
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def quarantine_and_alert(context: OpExecutionContext, quality_result: Dict[str, Any]) -> Dict[str, Any]:
    if quality_result["high_quality"]:
        context.log.info("Skipping quarantine: quality meets threshold")
        return {"status": "skipped"}
    
    file_path = quality_result["file_path"]
    context.log.info(f"Moving to quarantine: {file_path}")
    quarantine_path = move_to_quarantine_storage(file_path)
    
    return {
        "status": "quarantined",
        "quarantine_path": quarantine_path,
        "quality_score": quality_result["quality_score"],
    }


@op(
    description="Sends alert email for quarantined data",
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def send_alert_email(context: OpExecutionContext, quarantine_result: Dict[str, Any]) -> Dict[str, Any]:
    if quarantine_result["status"] != "quarantined":
        context.log.info("Skipping alert: no quarantine action")
        return {"status": "skipped"}
    
    quality_score = quarantine_result["quality_score"]
    quarantine_path = quarantine_result["quarantine_path"]
    context.log.info(f"Sending quality alert: {quality_score}%")
    send_quality_alert_email(quality_score, quarantine_path)
    return {"status": "alert_sent"}


@op(
    description="Performs final cleanup of temporary files and resources",
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def cleanup(
    context: OpExecutionContext,
    production_result: Dict[str, Any],
    alert_result: Dict[str, Any],
    quality_result: Dict[str, Any],
) -> Dict[str, Any]:
    file_path = quality_result["file_path"]
    if quality_result["high_quality"] and os.path.exists(file_path):
        cleanup_temp_resources(file_path)
        context.log.info(f"Cleaned up production file: {file_path}")
    else:
        context.log.info(f"File already moved to quarantine: {file_path}")
    return {"status": "cleanup_complete"}


@job(description="Data quality gate pipeline for customer CSV data")
def data_quality_gate_pipeline():
    file_metadata = ingest_csv()
    quality_result = quality_check(file_metadata)
    
    production_result = production_load(quality_result)
    quarantine_result = quarantine_and_alert(quality_result)
    alert_result = send_alert_email(quarantine_result)
    
    cleanup(production_result, alert_result, quality_result)


data_quality_gate_schedule = ScheduleDefinition(
    job=data_quality_gate_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
)

defs = Definitions(
    jobs=[data_quality_gate_pipeline],
    schedules=[data_quality_gate_schedule],
)


if __name__ == "__main__":
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,name,email\n")
        for i in range(100):
            f.write(f"{i},Customer {i},customer{i}@example.com\n")
        test_file = f.name
    
    try:
        result = data_quality_gate_pipeline.execute_in_process(
            run_config={"ops": {"ingest_csv": {"config": {"source_file_path": test_file}}}}
        )
        print(f"Pipeline execution {'succeeded' if result.success else 'failed'}")
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)