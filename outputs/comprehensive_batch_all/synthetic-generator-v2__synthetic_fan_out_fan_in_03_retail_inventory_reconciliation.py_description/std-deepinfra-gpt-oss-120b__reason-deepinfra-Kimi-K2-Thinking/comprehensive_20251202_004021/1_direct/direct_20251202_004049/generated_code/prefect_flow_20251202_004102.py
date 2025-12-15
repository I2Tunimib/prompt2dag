from __future__ import annotations

import os
import random
import string
from pathlib import Path
from typing import Dict, List

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta


WAREHOUSES = ["north", "south", "east", "west"]
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def _random_sku() -> str:
    """Generate a random SKU string."""
    prefix = random.choice(["A", "B", "C", "D"])
    suffix = "".join(random.choices(string.digits, k=5))
    return f"{prefix}{suffix}"


@task(retries=2, retry_delay_seconds=300, name="fetch_warehouse_csv")
def fetch_warehouse_csv(warehouse: str) -> Path:
    """
    Simulate fetching a CSV file from a warehouse system.

    Args:
        warehouse: Name of the warehouse.

    Returns:
        Path to the generated CSV file.
    """
    records = []
    for _ in range(1000):
        sku = _random_sku()
        quantity = random.randint(0, 100)
        records.append({"sku": sku, "quantity": quantity})

    df = pd.DataFrame(records)
    file_path = DATA_DIR / f"{warehouse}_raw.csv"
    df.to_csv(file_path, index=False)
    return file_path


@task(retries=2, retry_delay_seconds=300, name="normalize_sku")
def normalize_sku(csv_path: Path, warehouse: str) -> Path:
    """
    Normalize SKU formats for a given warehouse CSV.

    Args:
        csv_path: Path to the raw CSV file.
        warehouse: Name of the warehouse.

    Returns:
        Path to the normalized CSV file.
    """
    df = pd.read_csv(csv_path)

    # Apply transformation rules
    df["sku"] = (
        df["sku"]
        .astype(str)
        .str.upper()
        .str.strip()
        .apply(lambda x: f"STD-{x}" if not x.startswith("STD-") else x)
    )

    normalized_path = DATA_DIR / f"{warehouse}_normalized.csv"
    df.to_csv(normalized_path, index=False)
    return normalized_path


@task(retries=2, retry_delay_seconds=300, name="reconcile_inventory")
def reconcile_inventory(
    normalized_paths: Dict[str, Path],
) -> Path:
    """
    Reconcile inventory across all warehouses.

    Args:
        normalized_paths: Mapping of warehouse name to its normalized CSV path.

    Returns:
        Path to the discrepancy report CSV.
    """
    # Load all normalized dataframes
    dfs: List[pd.DataFrame] = []
    for wh, path in normalized_paths.items():
        df = pd.read_csv(path)
        df = df.rename(columns={"quantity": f"quantity_{wh}"})
        dfs.append(df)

    # Merge on SKU
    merged = dfs[0]
    for df in dfs[1:]:
        merged = pd.merge(
            merged,
            df,
            on="sku",
            how="outer",
        )

    # Fill missing quantities with 0
    quantity_cols = [col for col in merged.columns if col.startswith("quantity_")]
    merged[quantity_cols] = merged[quantity_cols].fillna(0).astype(int)

    # Identify mismatched SKUs (where not all quantities are equal)
    merged["max_qty"] = merged[quantity_cols].max(axis=1)
    merged["min_qty"] = merged[quantity_cols].min(axis=1)
    discrepancy_mask = merged["max_qty"] != merged["min_qty"]
    discrepancy_df = merged[discrepancy_mask].copy()
    discrepancy_df = discrepancy_df.drop(columns=["max_qty", "min_qty"])

    report_path = DATA_DIR / "discrepancy_report.csv"
    discrepancy_df.to_csv(report_path, index=False)
    return report_path


@task(retries=2, retry_delay_seconds=300, name="generate_final_report")
def generate_final_report(discrepancy_report_path: Path) -> Path:
    """
    Generate a final PDF reconciliation report from the discrepancy data.

    For simplicity, this implementation creates a plain text summary file.

    Args:
        discrepancy_report_path: Path to the CSV discrepancy report.

    Returns:
        Path to the final report file.
    """
    df = pd.read_csv(discrepancy_report_path)
    total_discrepancies = len(df)

    report_lines = [
        "Retail Inventory Reconciliation Report",
        f"Generated on: {datetime.utcnow().isoformat()}",
        f"Total SKUs with discrepancies: {total_discrepancies}",
        "",
        "Discrepancy Details:",
    ]

    for _, row in df.iterrows():
        sku = row["sku"]
        qtys = {col: row[col] for col in df.columns if col.startswith("quantity_")}
        report_lines.append(f"- SKU: {sku}")
        for wh, qty in qtys.items():
            report_lines.append(f"    {wh}: {qty}")

    final_report_path = DATA_DIR / "final_reconciliation_report.txt"
    final_report_path.write_text("\n".join(report_lines))
    return final_report_path


@flow(name="retail_inventory_reconciliation")
def inventory_reconciliation_flow() -> Path:
    """
    Orchestrates the retail inventory reconciliation pipeline.

    Returns:
        Path to the final reconciliation report.
    """
    # Parallel fetch
    fetch_futures = {
        wh: fetch_warehouse_csv.submit(wh) for wh in WAREHOUSES
    }

    # Wait for fetch results
    fetched_paths = {
        wh: fut.result() for wh, fut in fetch_futures.items()
    }

    # Parallel normalization
    normalize_futures = {
        wh: normalize_sku.submit(csv_path, wh)
        for wh, csv_path in fetched_paths.items()
    }

    normalized_paths = {
        wh: fut.result() for wh, fut in normalize_futures.items()
    }

    # Reconciliation (sequential)
    discrepancy_path = reconcile_inventory(normalized_paths)

    # Final report generation (sequential)
    final_report_path = generate_final_report(discrepancy_path)

    return final_report_path


# Note: Deployment configuration (e.g., daily schedule starting 2024-01-01,
# no catchup, max parallelism of 4) should be defined in the Prefect UI or
# via a deployment script using `prefect deployment create`.

if __name__ == "__main__":
    report_path = inventory_reconciliation_flow()
    print(f"Final reconciliation report generated at: {report_path}")