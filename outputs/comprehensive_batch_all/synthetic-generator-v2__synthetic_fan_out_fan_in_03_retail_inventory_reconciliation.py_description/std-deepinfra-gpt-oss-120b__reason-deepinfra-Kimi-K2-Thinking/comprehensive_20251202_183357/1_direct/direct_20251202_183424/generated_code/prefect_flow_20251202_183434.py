from pathlib import Path
from typing import List

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=2, retry_delay_seconds=300)
def fetch_warehouse_csv(warehouse: str) -> str:
    """
    Mock fetch of a CSV inventory file for a given warehouse.

    Args:
        warehouse: Name of the warehouse (e.g., "north").

    Returns:
        Path to the fetched CSV file as a string.
    """
    # In a real implementation, this would download the file from a remote system.
    csv_path = Path("/tmp") / f"{warehouse}_inventory.csv"
    # Simulate file creation
    csv_path.touch()
    return str(csv_path)


@task(retries=2, retry_delay_seconds=300)
def normalize_sku(csv_path: str, warehouse: str) -> str:
    """
    Normalize SKU formats in the given CSV file.

    Args:
        csv_path: Path to the raw CSV file.
        warehouse: Name of the warehouse (used for naming the output).

    Returns:
        Path to the normalized CSV file as a string.
    """
    normalized_path = Path("/tmp") / f"{warehouse}_inventory_normalized.csv"
    # Simulate normalization logic (uppercase, strip whitespace, prefix standardization)
    # In a real implementation, read csv_path, transform, and write to normalized_path.
    normalized_path.touch()
    return str(normalized_path)


@task(retries=2, retry_delay_seconds=300)
def reconcile_inventory(normalized_paths: List[str]) -> str:
    """
    Reconcile inventory across all warehouses and generate a discrepancy report.

    Args:
        normalized_paths: List of paths to normalized CSV files.

    Returns:
        Path to the discrepancy report file as a string.
    """
    discrepancy_path = Path("/tmp") / "inventory_discrepancy_report.csv"
    # Simulate reconciliation logic: compare SKU counts, identify mismatches, write report.
    discrepancy_path.touch()
    return str(discrepancy_path)


@task(retries=2, retry_delay_seconds=300)
def generate_final_report(discrepancy_report_path: str) -> str:
    """
    Generate the final PDF reconciliation report from the discrepancy data.

    Args:
        discrepancy_report_path: Path to the CSV discrepancy report.

    Returns:
        Path to the final PDF report as a string.
    """
    pdf_path = Path("/tmp") / "inventory_reconciliation_report.pdf"
    # Simulate PDF generation.
    pdf_path.touch()
    return str(pdf_path)


@flow
def inventory_reconciliation_flow() -> str:
    """
    Orchestrates the retail inventory reconciliation pipeline.

    Returns:
        Path to the final PDF reconciliation report.
    """
    warehouses = ["north", "south", "east", "west"]

    # Parallel fetch of CSV files
    fetch_futures = {
        warehouse: fetch_warehouse_csv.submit(warehouse) for warehouse in warehouses
    }
    fetched_paths = {
        warehouse: future.result() for warehouse, future in fetch_futures.items()
    }

    # Parallel normalization of SKU formats
    normalize_futures = {
        warehouse: normalize_sku.submit(fetched_paths[warehouse], warehouse)
        for warehouse in warehouses
    }
    normalized_paths = [future.result() for future in normalize_futures.values()]

    # Reconcile inventory discrepancies
    discrepancy_path = reconcile_inventory(normalized_paths)

    # Generate final PDF report
    final_report_path = generate_final_report(discrepancy_path)

    return final_report_path


# Note: Deployment configuration (e.g., daily schedule starting 2024-01-01,
# no catchup, max parallel width of 4) should be defined in the Prefect
# deployment YAML or UI, not in code.

if __name__ == "__main__":
    report_path = inventory_reconciliation_flow()
    print(f"Final reconciliation report generated at: {report_path}")