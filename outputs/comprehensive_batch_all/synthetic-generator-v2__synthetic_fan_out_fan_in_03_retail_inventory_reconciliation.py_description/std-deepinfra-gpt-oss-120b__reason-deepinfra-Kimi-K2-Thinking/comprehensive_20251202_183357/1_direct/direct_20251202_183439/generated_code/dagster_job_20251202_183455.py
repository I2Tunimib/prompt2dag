from datetime import datetime, timedelta

from dagster import (
    In,
    Out,
    RetryPolicy,
    RunRequest,
    DefaultScheduleStatus,
    job,
    op,
    schedule,
)


@op(
    name="fetch_north",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(str),
)
def fetch_north() -> str:
    """Simulate fetching the CSV file from the north warehouse."""
    # In a real implementation, this would download a file from an external system.
    return "data/north_raw.csv"


@op(
    name="fetch_south",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(str),
)
def fetch_south() -> str:
    """Simulate fetching the CSV file from the south warehouse."""
    return "data/south_raw.csv"


@op(
    name="fetch_east",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(str),
)
def fetch_east() -> str:
    """Simulate fetching the CSV file from the east warehouse."""
    return "data/east_raw.csv"


@op(
    name="fetch_west",
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(str),
)
def fetch_west() -> str:
    """Simulate fetching the CSV file from the west warehouse."""
    return "data/west_raw.csv"


@op(
    name="normalize_north",
    ins={"raw_path": In(str)},
    out=Out(str),
)
def normalize_north(raw_path: str) -> str:
    """Normalize SKU formats for the north warehouse."""
    # Placeholder transformation logic.
    return raw_path.replace("_raw", "_norm")


@op(
    name="normalize_south",
    ins={"raw_path": In(str)},
    out=Out(str),
)
def normalize_south(raw_path: str) -> str:
    """Normalize SKU formats for the south warehouse."""
    return raw_path.replace("_raw", "_norm")


@op(
    name="normalize_east",
    ins={"raw_path": In(str)},
    out=Out(str),
)
def normalize_east(raw_path: str) -> str:
    """Normalize SKU formats for the east warehouse."""
    return raw_path.replace("_raw", "_norm")


@op(
    name="normalize_west",
    ins={"raw_path": In(str)},
    out=Out(str),
)
def normalize_west(raw_path: str) -> str:
    """Normalize SKU formats for the west warehouse."""
    return raw_path.replace("_raw", "_norm")


@op(
    name="reconcile_inventory",
    ins={
        "north": In(str),
        "south": In(str),
        "east": In(str),
        "west": In(str),
    },
    out=Out(str),
)
def reconcile_inventory(north: str, south: str, east: str, west: str) -> str:
    """Compare normalized datasets and generate a discrepancy report."""
    # In a real implementation, this would read the normalized CSVs,
    # compare SKU quantities, and write a discrepancy CSV.
    return "reports/discrepancy_report.csv"


@op(
    name="generate_final_report",
    ins={"discrepancy_path": In(str)},
    out=Out(str),
)
def generate_final_report(discrepancy_path: str) -> str:
    """Create a comprehensive PDF report from the discrepancy data."""
    # Placeholder for PDF generation logic.
    return "reports/final_reconciliation_report.pdf"


@job
def inventory_reconciliation_job():
    """Orchestrates the full inventory reconciliation pipeline."""
    # Fetch phase (parallel)
    north_raw = fetch_north()
    south_raw = fetch_south()
    east_raw = fetch_east()
    west_raw = fetch_west()

    # Normalization phase (parallel)
    north_norm = normalize_north(north_raw)
    south_norm = normalize_south(south_raw)
    east_norm = normalize_east(east_raw)
    west_norm = normalize_west(west_raw)

    # Reconciliation phase (fan‑in)
    discrepancy_report = reconcile_inventory(
        north=north_norm,
        south=south_norm,
        east=east_norm,
        west=west_norm,
    )

    # Final report generation
    final_report = generate_final_report(discrepancy_report)

    # The final_report output can be used downstream or for monitoring.
    return final_report


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=inventory_reconciliation_job,
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily inventory reconciliation schedule",
)
def daily_inventory_reconciliation_schedule(_context):
    """Schedule that triggers the inventory reconciliation job each day."""
    return RunRequest(run_key=None, run_config={})


if __name__ == "__main__":
    result = inventory_reconciliation_job.execute_in_process()
    print("Job succeeded:", result.success)