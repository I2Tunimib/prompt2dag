from datetime import datetime, timedelta
from typing import List

import pandas as pd
from dagster import (
    DefaultScheduleStatus,
    RetryPolicy,
    ScheduleDefinition,
    JobDefinition,
    job,
    op,
    schedule,
)


def _mock_fetch_csv(warehouse: str) -> pd.DataFrame:
    """Simulate fetching a CSV file for a warehouse."""
    data = {
        "SKU": [f"{warehouse[:1]}-sku-{i}" for i in range(1, 6)],
        "quantity": [10 + i for i in range(5)],
    }
    return pd.DataFrame(data)


def _normalize_sku(sku: str) -> str:
    """Apply standard SKU transformations."""
    sku = sku.upper().strip()
    if not sku.startswith("SKU-"):
        sku = f"SKU-{sku}"
    return sku


def _mock_generate_pdf(report_content: str, path: str) -> None:
    """Simulate PDF generation by writing plain text."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(report_content)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Fetch inventory CSV from the North warehouse.",
)
def fetch_north() -> pd.DataFrame:
    return _mock_fetch_csv("north")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Fetch inventory CSV from the South warehouse.",
)
def fetch_south() -> pd.DataFrame:
    return _mock_fetch_csv("south")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Fetch inventory CSV from the East warehouse.",
)
def fetch_east() -> pd.DataFrame:
    return _mock_fetch_csv("east")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Fetch inventory CSV from the West warehouse.",
)
def fetch_west() -> pd.DataFrame:
    return _mock_fetch_csv("west")


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["SKU"] = df["SKU"].apply(_normalize_sku)
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Normalize SKU formats for the North warehouse.",
)
def normalize_north(raw_north: pd.DataFrame) -> pd.DataFrame:
    return _normalize(raw_north)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Normalize SKU formats for the South warehouse.",
)
def normalize_south(raw_south: pd.DataFrame) -> pd.DataFrame:
    return _normalize(raw_south)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Normalize SKU formats for the East warehouse.",
)
def normalize_east(raw_east: pd.DataFrame) -> pd.DataFrame:
    return _normalize(raw_east)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Normalize SKU formats for the West warehouse.",
)
def normalize_west(raw_west: pd.DataFrame) -> pd.DataFrame:
    return _normalize(raw_west)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Reconcile inventory discrepancies across all warehouses.",
)
def reconcile_inventory(
    north: pd.DataFrame,
    south: pd.DataFrame,
    east: pd.DataFrame,
    west: pd.DataFrame,
) -> pd.DataFrame:
    """Identify quantity mismatches across warehouses."""
    # Tag each row with its source warehouse
    north["warehouse"] = "north"
    south["warehouse"] = "south"
    east["warehouse"] = "east"
    west["warehouse"] = "west"

    combined = pd.concat([north, south, east, west], ignore_index=True)

    # Pivot to have quantities per warehouse per SKU
    pivot = combined.pivot_table(
        index="SKU",
        columns="warehouse",
        values="quantity",
        aggfunc="first",
    ).reset_index()

    # Identify rows where not all quantities are equal (i.e., mismatches)
    mismatch_mask = pivot.nunique(axis=1) > 1
    mismatches = pivot[mismatch_mask].copy()
    mismatches["discrepancy"] = "quantity_mismatch"

    return mismatches


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Generate the final PDF reconciliation report.",
)
def generate_final_report(discrepancies: pd.DataFrame) -> str:
    """Create a simple text representation of the report and simulate PDF output."""
    if discrepancies.empty:
        report_body = "No inventory discrepancies detected."
    else:
        lines: List[str] = ["Inventory Discrepancy Report", "-" * 30]
        for _, row in discrepancies.iterrows():
            sku = row["SKU"]
            qtys = {col: row[col] for col in ["north", "south", "east", "west"] if pd.notna(row[col])}
            line = f"SKU: {sku} | Quantities: {qtys}"
            lines.append(line)
        report_body = "\n".join(lines)

    pdf_path = "reconciliation_report.pdf"
    _mock_generate_pdf(report_body, pdf_path)
    return pdf_path


@job(description="Retail inventory reconciliation pipeline.")
def inventory_reconciliation_job():
    # Fetch phase (parallel)
    raw_north = fetch_north()
    raw_south = fetch_south()
    raw_east = fetch_east()
    raw_west = fetch_west()

    # Normalization phase (parallel, each depends on its fetch)
    norm_north = normalize_north(raw_north)
    norm_south = normalize_south(raw_south)
    norm_east = normalize_east(raw_east)
    norm_west = normalize_west(raw_west)

    # Reconciliation phase (fan‑in)
    discrepancies = reconcile_inventory(
        north=norm_north,
        south=norm_south,
        east=norm_east,
        west=norm_west,
    )

    # Final report generation
    generate_final_report(discrepancies)


daily_inventory_schedule = ScheduleDefinition(
    job=inventory_reconciliation_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily schedule for the inventory reconciliation job.",
)


if __name__ == "__main__":
    result = inventory_reconciliation_job.execute_in_process()
    if result.success:
        print("Inventory reconciliation job completed successfully.")
    else:
        print("Inventory reconciliation job failed.")