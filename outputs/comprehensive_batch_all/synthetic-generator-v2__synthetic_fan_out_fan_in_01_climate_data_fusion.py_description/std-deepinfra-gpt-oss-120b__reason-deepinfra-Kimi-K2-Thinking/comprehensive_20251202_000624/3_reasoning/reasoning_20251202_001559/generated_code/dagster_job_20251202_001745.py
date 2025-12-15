from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    ConfigurableResource,
    In,
    Out,
    failure_hook,
    HookContext,
)
from typing import List
import os
import tempfile
import pandas as pd


class FileStorageResource(ConfigurableResource):
    """Resource for managing file storage paths."""
    base_path: str = tempfile.gettempdir()
    
    def get_path(self, filename: str) -> str:
        return os.path.join(self.base_path, filename)


class EmailNotificationResource(ConfigurableResource):
    """Resource for sending email notifications on failures."""
    smtp_host: str = "localhost"
    smtp_port: int = 587
    from_email: str = "dagster@example.com"
    to_emails: List[str] = ["admin@example.com"]
    
    def send_failure_alert(self, op_name: str, error: str) -> None:
        # Minimal stub - implement actual SMTP logic in production
        print(f"ALERT: Failure in {op_name} - {error}")


@failure_hook(required_resource_keys={"email_notifier"})
def email_on_failure(context: HookContext) -> None:
    """Hook to send email notifications when ops fail."""
    error_msg = str(context.op_exception) if context.op_exception else "Unknown error"
    context.resources.email_notifier.send_failure_alert(context.op.name, error_msg)


@op(
    out=Out(str, description="Path to downloaded NOAA CSV file"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    hooks={email_on_failure},
    tags={"phase": "download"}
)
def download_noaa(storage: FileStorageResource) -> str:
    """Fetches NOAA weather station CSV data from FTP server."""
    output_path = storage.get_path("noaa_raw.csv")
    pd.DataFrame({
        'timestamp': ['2023-01-01T00:00:00Z'],
        'temp_f': [32.0],
        'pressure_in': [29.92]
    }).to_csv(output_path, index=False)
    return output_path


@op(
    out=Out(str, description="Path to