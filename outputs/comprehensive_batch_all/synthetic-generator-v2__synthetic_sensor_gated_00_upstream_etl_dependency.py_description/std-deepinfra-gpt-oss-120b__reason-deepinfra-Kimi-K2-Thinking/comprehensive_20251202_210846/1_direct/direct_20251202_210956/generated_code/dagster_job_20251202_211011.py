from datetime import datetime, timedelta
import os
import csv

from dagster import (
    op,
    job,
    RetryPolicy,
    sensor,
    RunRequest,
    SkipReason,
    schedule,
    Definitions,
    ConfigurableResource,
    get_dagster_logger,
)


class EmailNotifierResource(ConfigurableResource):
    """Stub email notifier resource."""

    smtp_server: str = "localhost"
    from_addr: str = "no-reply@example.com"

    def send_failure(self, subject: str, body: str) -> None:
        logger = get_dagster_logger()
        logger.info(f"[EmailNotifier] Subject: {subject}\nBody: {body}")


@op(
    config_schema={"csv_path": str},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"email_notifier"},
)
def load_sales_csv(context) -> list[dict]:
    """Load and validate the aggregated sales CSV produced by the upstream DAG."""
    csv_path = context.op_config["csv_path"]
    logger = context.log

    if not os.path.isfile(csv_path):
        error_msg = f"CSV file not found at path: {csv_path}"
        logger.error(error_msg)
        context.resources.email_notifier.send_failure(
            subject="Sales Dashboard Failure: Missing CSV",
            body=error_msg,
        )
        raise FileNotFoundError(error_msg)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        error_msg = f"CSV file at {csv_path} is empty."
        logger.error(error_msg)
        context.resources.email_notifier.send_failure(
            subject="Sales Dashboard Failure: Empty CSV",
            body=error_msg,
        )
        raise ValueError(error_msg)

    logger.info(f"Loaded {len(rows)} rows from {csv_path}.")
    return rows


@op
def generate_dashboard(context, sales_data: list[dict]) -> dict:
    """Generate a simple executive dashboard from the sales data."""
    logger = context.log

    # Compute a basic metric: total revenue.
    total_revenue = 0.0
    for row in sales_data:
        try:
            total_revenue += float(row.get("revenue", 0))
        except ValueError:
            continue

    logger.info(f"Dashboard generated – total revenue: ${total_revenue:,.2f}")

    # In a real implementation, visualizations would be created here.
    return {"total_revenue": total_revenue}


@job(resource_defs={"email_notifier": EmailNotifierResource()})
def executive_sales_dashboard():
    """Orchestrates loading sales data and generating the executive dashboard."""
    sales = load_sales_csv()
    generate_dashboard(sales)


def _upstream_success_indicator_path() -> str:
    """Path used as a simple indicator that the upstream aggregation succeeded."""
    return "/tmp/daily_sales_aggregation_success.txt"


@sensor(job=executive_sales_dashboard, minimum_interval_seconds=60)
def wait_for_sales_aggregation(context):
    """
    Sensor that triggers the dashboard job once the upstream daily_sales_aggregation
    process signals completion via a marker file.
    """
    indicator_path = _upstream_success_indicator_path()
    if not os.path.isfile(indicator_path):
        return SkipReason("Upstream aggregation marker file not found.")

    # Ensure the marker file is recent (within the last hour).
    file_mtime = datetime.fromtimestamp(os.path.getmtime(indicator_path))
    if datetime.utcnow() - file_mtime > timedelta(hours=1):
        return SkipReason("Upstream aggregation marker file is stale.")

    return RunRequest(
        run_key="sales_dashboard_run",
        run_config={
            "ops": {
                "load_sales_csv": {
                    "config": {"csv_path": "/tmp/aggregated_sales.csv"}
                }
            }
        },
    )


@schedule(cron_schedule="0 2 * * *", job=executive_sales_dashboard, execution_timezone="UTC")
def daily_sales_dashboard_schedule():
    """Daily schedule aligned with the upstream aggregation window."""
    return {
        "ops": {
            "load_sales_csv": {
                "config": {"csv_path": "/tmp/aggregated_sales.csv"}
            }
        }
    }


defs = Definitions(
    jobs=[executive_sales_dashboard],
    sensors=[wait_for_sales_aggregation],
    schedules=[daily_sales_dashboard_schedule],
    resources={"email_notifier": EmailNotifierResource()},
)


if __name__ == "__main__":
    result = executive_sales_dashboard.execute_in_process(
        run_config={
            "ops": {
                "load_sales_csv": {"config": {"csv_path": "/tmp/aggregated_sales.csv"}}
            }
        }
    )
    assert result.success