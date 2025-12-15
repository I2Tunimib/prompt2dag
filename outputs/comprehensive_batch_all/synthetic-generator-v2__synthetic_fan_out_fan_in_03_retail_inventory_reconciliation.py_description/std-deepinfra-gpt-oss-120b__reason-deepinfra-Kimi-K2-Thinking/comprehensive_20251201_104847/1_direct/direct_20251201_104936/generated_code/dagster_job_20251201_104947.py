from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict

from dagster import (
    Definitions,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    DefaultScheduleStatus,
    JobDefinition,
    job,
    op,
    get_dagster_logger,
)


def _mock_fetch_csv(warehouse: str) -> str:
    """Simulate fetching a CSV file for a warehouse."""
    logger = get_dagster_logger()
    logger.info(f"Fetching CSV for {warehouse} warehouse.")
    # In a real implementation this would download a file.
    # Here we just create a dummy path.
    file_path = Path(f"/tmp/{warehouse}_inventory.csv")
    file_path.write_text("sku,quantity\nSKU001,100\nSKU002,200\n")
    return str(file_path)


def _mock_normalize_csv(csv_path: str) -> List[Dict[str, str]]:
    """Simulate normalizing SKU formats from a CSV file."""
    logger = get_dagster_logger()
    logger.info(f"Normalizing data from {csv_path}.")
    # Dummy normalization: read lines and apply simple rules.
    normalized = []
    with open(csv_path, "r") as f:
        next(f)  # skip header
        for line in f:
            sku, qty = line.strip().split(",")
            sku_std = sku.upper().strip()
            normalized.append({"sku": sku_std, "quantity": int(qty)})
    return normalized


def _mock_reconcile(
    north: List[Dict[str, str]],
    south: List[Dict[str, str]],
    east: List[Dict[str, str]],
    west: List[Dict[str, str]],
) -> str:
    """Compare normalized inventories and generate a discrepancy report."""
    logger = get_dagster_logger()
    logger.info("Reconciling inventories across warehouses.")
    # Simple aggregation to find mismatched quantities.
    sku_totals: Dict[str, List[int]] = {}
    for dataset in (north, south, east, west):
        for record in dataset:
            sku = record["sku"]
            qty = record["quantity"]
            sku_totals.setdefault(sku, []).append(qty)

    discrepancies = [
        {"sku": sku, "quantities": qtys}
        for sku, qtys in sku_totals.items()
        if len(set(qtys)) > 1
    ]

    report_path = Path("/tmp/discrepancy_report.csv")
    with report_path.open("w") as f:
        f.write("sku,quantities\n")
        for d in discrepancies:
            f.write(f"{d['sku']},\"{d['quantities']}\"\n")
    logger.info(f"Discrepancy report written to {report_path}.")
    return str(report_path)


def _mock_generate_pdf(report_csv_path: str) -> str:
    """Create a PDF report from the discrepancy CSV."""
    logger = get_dagster_logger()
    logger.info(f"Generating PDF report from {report_csv_path}.")
    pdf_path = Path("/tmp/final_reconciliation_report.pdf")
    # In a real implementation, convert CSV to PDF.
    pdf_path.write_text(f"PDF report based on {report_csv_path}\n")
    logger.info(f"PDF report written to {pdf_path}.")
    return str(pdf_path)


# Retry policy shared by all ops
default_retry = RetryPolicy(max_retries=2, delay=timedelta(minutes=5))


@op(out=Out(str), retry_policy=default_retry)
def fetch_north() -> str:
    """Fetch CSV for the north warehouse."""
    return _mock_fetch_csv("north")


@op(out=Out(str), retry_policy=default_retry)
def fetch_south() -> str:
    """Fetch CSV for the south warehouse."""
    return _mock_fetch_csv("south")


@op(out=Out(str), retry_policy=default_retry)
def fetch_east() -> str:
    """Fetch CSV for the east warehouse."""
    return _mock_fetch_csv("east")


@op(out=Out(str), retry_policy=default_retry)
def fetch_west() -> str:
    """Fetch CSV for the west warehouse."""
    return _mock_fetch_csv("west")


@op(ins={"csv_path": In(str)}, out=Out(list), retry_policy=default_retry)
def normalize_north(csv_path: str) -> List[Dict[str, str]]:
    """Normalize north warehouse data."""
    return _mock_normalize_csv(csv_path)


@op(ins={"csv_path": In(str)}, out=Out(list), retry_policy=default_retry)
def normalize_south(csv_path: str) -> List[Dict[str, str]]:
    """Normalize south warehouse data."""
    return _mock_normalize_csv(csv_path)


@op(ins={"csv_path": In(str)}, out=Out(list), retry_policy=default_retry)
def normalize_east(csv_path: str) -> List[Dict[str, str]]:
    """Normalize east warehouse data."""
    return _mock_normalize_csv(csv_path)


@op(ins={"csv_path": In(str)}, out=Out(list), retry_policy=default_retry)
def normalize_west(csv_path: str) -> List[Dict[str, str]]:
    """Normalize west warehouse data."""
    return _mock_normalize_csv(csv_path)


@op(
    ins={
        "north": In(list),
        "south": In(list),
        "east": In(list),
        "west": In(list),
    },
    out=Out(str),
    retry_policy=default_retry,
)
def reconcile_inventory(
    north: List[Dict[str, str]],
    south: List[Dict[str, str]],
    east: List[Dict[str, str]],
    west: List[Dict[str, str]],
) -> str:
    """Reconcile inventories and produce a discrepancy CSV."""
    return _mock_reconcile(north, south, east, west)


@op(ins={"discrepancy_report": In(str)}, out=Out(str), retry_policy=default_retry)
def generate_final_report(discrepancy_report: str) -> str:
    """Generate the final PDF reconciliation report."""
    return _mock_generate_pdf(discrepancy_report)


@job
def inventory_reconciliation_job() -> None:
    """Orchestrates the full inventory reconciliation pipeline."""
    # Fetch phase (parallel)
    north_csv = fetch_north()
    south_csv = fetch_south()
    east_csv = fetch_east()
    west_csv = fetch_west()

    # Normalization phase (parallel, each depends on its fetch)
    north_norm = normalize_north(north_csv)
    south_norm = normalize_south(south_csv)
    east_norm = normalize_east(east_csv)
    west_norm = normalize_west(west_csv)

    # Reconciliation (fan‑in)
    discrepancy_csv = reconcile_inventory(
        north=north_norm,
        south=south_norm,
        east=east_norm,
        west=west_norm,
    )

    # Final report generation
    generate_final_report(discrepancy_csv)


daily_inventory_schedule = ScheduleDefinition(
    job=inventory_reconciliation_job,
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily inventory reconciliation across all warehouses.",
)


defs = Definitions(
    jobs=[inventory_reconciliation_job],
    schedules=[daily_inventory_schedule],
)


if __name__ == "__main__":
    result = inventory_reconciliation_job.execute_in_process()
    if result.success:
        print("Inventory reconciliation completed successfully.")
    else:
        print("Inventory reconciliation failed.")