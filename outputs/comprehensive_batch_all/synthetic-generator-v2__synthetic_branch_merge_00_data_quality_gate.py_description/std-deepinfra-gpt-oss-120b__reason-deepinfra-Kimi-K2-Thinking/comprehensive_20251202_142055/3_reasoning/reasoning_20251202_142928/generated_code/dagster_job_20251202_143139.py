from dagster import (
    op,
    job,
    RetryPolicy,
    Out,
    In,
    Nothing,
)
import os
import shutil
from typing import Tuple, Dict, Any


# Minimal resource stubs for production use:
# @resource
# def database_resource(init_context):
#     return DatabaseConnection()
#
# @resource
# def email_resource(init_context):
#     return EmailClient()
#
# @resource
# def file_store_resource(init_context):
#     return FileStore()


@op(
    out={"file_metadata": Out(dict), "file_path": Out(str)},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def ingest_csv(context) -> Tuple[Dict[str, Any], str]:
    """Ingests raw customer CSV data and returns metadata."""
    # In production, use configurable source via resources/config
    source_location = "/data/customers.csv"
    temp_location = "/tmp/ingested_customers.csv"
    
    if os.path.exists(source_location):
        shutil.copy2(source_location, temp_location)
    
    file_metadata = {
        "source": source_location,
        "size": os.path.getsize(temp_location) if os.path.exists(temp_location) else 0,
        "rows": 1000,
    }
    
    context.log.info(f"Ingested CSV from {source_location}")
    return file_metadata, temp_location


@op(
    out={"quality_result": Out(dict)},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def quality_check(context, file_metadata: Dict[str, Any], file_path: str) -> Dict[str, Any]:
    """Calculates data quality score and returns routing information."""
    completeness_score = 0.96
    validity_score = 0.94
    
    quality_score = (completeness_score + validity_score) / 2 * 100
    
    result = {
        "quality_score": quality_score,
        "threshold": 95.0,
        "passed": quality_score >= 95.0,
        "file_metadata": file_metadata,
        "file_path": file_path,
    }
    
    context.log.info(f"Quality score: {quality_score:.2f}%")
    return result


@op(retry_policy=RetryPolicy(max_retries=1, delay=300))
def production_load(context, quality_result: Dict[str, Any]) -> None:
    """Loads data to production database if quality threshold is met."""
    if not quality_result["passed"]:
        context.log.info("Quality check failed, skipping production load")
        return
    
    file_path = quality_result["file_path"]
    # In production: context.resources.database.load_data(file_path)
    context.log.info(f"Loading {file_path} to production database")


@op(
    out={"quarantined": Out(bool)},
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def quarantine_and_alert(context, quality_result: Dict[str, Any]) -> bool:
    """Quarantines data and returns flag if quality threshold is not met."""
    if quality_result["passed"]:
        context.log.info("Quality check passed, skipping quarantine")
        return False
    
    file_path = quality_result["file_path"]
    quarantine_path = "/quarantine/low_quality_data.csv"
    
    os.makedirs("/quarantine", exist_ok=True)
    shutil.move(file_path, quarantine_path)
    
    context.log.info(f"Quarantined {file_path} to {quarantine_path}")
    return True


@op(retry_policy=RetryPolicy(max_retries=1, delay=300))
def send_alert_email(context, quarantined: bool) -> None:
    """Sends alert email to data stewards if data was quarantined."""
    if not quarantined:
        context.log.info("No quarantine needed, skipping alert")
        return
    
    # In production: context.resources.email.send_alert("Data quarantined")
    context.log.info("Sending alert email to data stewards")


@op(
    ins={
        "production_load_result": In(Nothing),
        "send_alert_email_result": In(Nothing),
    },
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def cleanup(context, production_load_result, send_alert_email_result):
    """Performs final cleanup of temporary files and resources."""
    temp_files = ["/tmp/ingested_customers.csv"]
    
    for file_path in temp_files:
        if os.path.exists(file_path):
            os.remove(file_path)
            context.log.info(f"Cleaned up {file_path}")
    
    context.log.info("Cleanup completed")


@job
def data_quality_gate_pipeline():
    """
    Data quality gate pipeline that routes customer CSV data to production or quarantine
    based on quality scores. Executes daily with conditional branching.
    """
    file_metadata, file_path = ingest_csv()
    quality_result = quality_check(file_metadata, file_path)
    
    production_load_result = production_load(quality_result)
    quarantined = quarantine_and_alert(quality_result)
    
    send_alert_email_result = send_alert_email(quarantined)
    
    cleanup(production_load_result, send_alert_email_result)


if __name__ == "__main__":
    # Manual execution pattern
    # For daily schedule, define: schedule = ScheduleDefinition(job=data_quality_gate_pipeline, cron_schedule="0 0 * * *")
    result = data_quality_gate_pipeline.execute_in_process()
    
    if result.success:
        print("Pipeline execution successful!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")