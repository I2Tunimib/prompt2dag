from pathlib import Path
from typing import Dict

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=2, retry_delay_seconds=300)
def fetch_warehouse_csv(warehouse: str) -> pd.DataFrame:
    """
    Mock fetch of a CSV inventory file for a given warehouse.

    Args:
        warehouse: Name of the warehouse (e.g., "north").

    Returns:
        DataFrame containing mock inventory data.
    """
    # In a real implementation, this would download or read a CSV file.
    # Here we generate a simple DataFrame with dummy SKU and quantity data.
    data = {
        "sku": [f"{warehouse[:1].upper()}-sku-{i:04d}" for i in range(1, 1001)],
        "quantity": [i % 50 + 1 for i in range(1, 1001)],
    }
    df = pd.DataFrame(data)
    return df


@task(retries=2, retry_delay_seconds=300)
def normalize_sku(df: pd.DataFrame, warehouse: str) -> pd.DataFrame:
    """
    Normalize SKU formats for a warehouse's inventory DataFrame.

    Transformations applied:
    - Uppercase conversion
    - Whitespace stripping
    - Prefix standardization to "SKU-"

    Args:
        df: Raw inventory DataFrame.
        warehouse: Warehouse name (used for logging).

    Returns:
        Normalized inventory DataFrame.
    """
    normalized = df.copy()
    # Uppercase and strip whitespace
    normalized["sku"] = (
        normalized["sku"]
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\s+", "", regex=True)
    )
    # Ensure prefix "SKU-"
    normalized["sku"] = normalized["sku"].apply(
        lambda x: f"SKU-{x}" if not x.startswith("SKU-") else x
    )
    return normalized


@task(retries=2, retry_delay_seconds=300)
def reconcile_inventory(normalized_data: Dict[str, pd.DataFrame]) -> Path:
    """
    Reconcile inventory across all warehouses.

    Identifies quantity mismatches and writes a discrepancy report.

    Args:
        normalized_data: Mapping of warehouse name to its normalized DataFrame.

    Returns:
        Path to the generated discrepancy CSV report.
    """
    # Concatenate all normalized data with a warehouse identifier
    combined = pd.concat(
        [
            df.assign(warehouse=warehouse)
            for warehouse, df in normalized_data.items()
        ],
        ignore_index=True,
    )

    # Compute total quantity per SKU across warehouses
    sku_summary = (
        combined.groupby("sku")["quantity"]
        .agg(["sum", "count"])
        .reset_index()
    )
    # Identify SKUs where quantity differs across warehouses (count > 1)
    mismatched = sku_summary[sku_summary["count"] > 1]

    # For demonstration, write mismatched SKUs to a CSV file
    report_path = Path("reports/discrepancy_report.csv")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    mismatched.to_csv(report_path, index=False)

    return report_path


@task(retries=2, retry_delay_seconds=300)
def generate_final_report(discrepancy_report_path: Path) -> Path:
    """
    Generate a comprehensive PDF reconciliation report.

    Args:
        discrepancy_report_path: Path to the CSV discrepancy report.

    Returns:
        Path to the final PDF report.
    """
    # In a real implementation, this would convert the CSV data into a PDF.
    # Here we create a placeholder PDF file.
    final_report_path = Path("reports/final_reconciliation_report.pdf")
    final_report_path.parent.mkdir(parents=True, exist_ok=True)
    with final_report_path.open("wb") as f:
        f.write(b"%PDF-1.4\n%Mock PDF content for reconciliation report.\n")
    return final_report_path


@flow
def inventory_reconciliation_flow() -> Path:
    """
    Orchestrates the retail inventory reconciliation pipeline.

    Steps:
    1. Parallel fetch of CSV files from four warehouses.
    2. Parallel normalization of SKU formats.
    3. Reconciliation of inventory discrepancies.
    4. Generation of the final PDF report.

    Returns:
        Path to the final PDF reconciliation report.
    """
    warehouses = ["north", "south", "east", "west"]

    # Parallel fetch
    fetch_futures = {
        wh: fetch_warehouse_csv.submit(wh) for wh in warehouses
    }
    fetched_data = {
        wh: future.result() for wh, future in fetch_futures.items()
    }

    # Parallel normalization
    normalize_futures = {
        wh: normalize_sku.submit(df, wh)
        for wh, df in fetched_data.items()
    }
    normalized_data = {
        wh: future.result() for wh, future in normalize_futures.items()
    }

    # Reconciliation (sequential)
    discrepancy_path = reconcile_inventory(normalized_data)

    # Final report generation (sequential)
    final_report_path = generate_final_report(discrepancy_path)

    return final_report_path


# Schedule configuration (to be applied during deployment):
# - Daily execution starting 2024-01-01
# - No catchup for missed runs
# - Maximum parallel width: 4 tasks (handled by Prefect's default concurrency)

if __name__ == "__main__":
    report_path = inventory_reconciliation_flow()
    print(f"Final reconciliation report generated at: {report_path}")