from dagster import (
    op,
    job,
    ScheduleDefinition,
    DefaultScheduleStatus,
    RetryPolicy,
    In,
    Out,
    get_dagster_logger,
)
from datetime import datetime
import pandas as pd
import os
import json
from typing import Dict, Any


class FileStorage:
    """Simple file storage utility for managing pipeline artifacts."""
    
    def __init__(self, base_path: str = "/tmp/warehouse_inventory"):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)
    
    def save(self, filename: str, data: pd.DataFrame) -> str:
        """Save DataFrame to CSV and return file path."""
        filepath = os.path.join(self.base_path, filename)
        data.to_csv(filepath, index=False)
        return filepath
    
    def load(self, filename: str) -> pd.DataFrame:
        """Load DataFrame from CSV file path."""
        filepath = os.path.join(self.base_path, filename)
        return pd.read_csv(filepath)


class PDFReporter:
    """Stub PDF reporter - replace with actual PDF generation library."""
    
    def generate_report(self, discrepancy_data: Dict[str, Any], output_path: str) -> str:
        """Generate a simple text-based report stub."""
        report_content = f"""INVENTORY RECONCILIATION REPORT
Generated: {datetime.now().isoformat()}
Total SKUs Checked: {discrepancy_data.get('total_skus_checked', 0)}
Discrepancies Found: {discrepancy_data.get('discrepancies_found', 0)}

DISCREPANCY DETAILS:
{json.dumps(discrepancy_data.get('discrepancies', []), indent=2)}

RECOMMENDATIONS:
1. Review SKUs with quantity variances
2. Investigate warehouse-specific discrepancies
3. Update inventory records to reflect actual counts
"""
        with open(output_path, 'w') as f:
            f.write(report_content)
        return output_path


@op(
    out=Out(str, description="Path to fetched warehouse CSV file"),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    config_schema={"warehouse": str},
)
def fetch_warehouse_csv(context) -> str:
    """Fetch inventory CSV file from a specific warehouse."""
    warehouse = context.op_config["warehouse"]
    logger = get_dagster_logger()
    logger.info(f"Fetching inventory data for {warehouse} warehouse")
    
    # Generate mock inventory data (1000+ SKUs)
    skus = [f"SKU_{i:04d}" for i in range(1, 1100)]
    quantities = [100 + (i % 50) for i in range(len(skus))]
    
    # Apply warehouse-specific SKU formatting
    warehouse_prefix = f"{warehouse[0].upper()}_"
    formatted_skus = [f"{warehouse_prefix}{sku}" for sku in skus]
    
    df = pd.DataFrame({
        "sku": formatted_skus,
        "quantity": quantities,
        "warehouse": [warehouse] * len(skus)
    })
    
    storage = FileStorage()
    filename = f"{warehouse}_inventory_{datetime.now().strftime('%Y%m%d')}.csv"
    filepath = storage.save(filename, df)
    
    logger.info(f"Fetched {len(df)} records for {warehouse} warehouse")
    return filepath


@op(
    ins={"csv_path": In(str, description="Path to warehouse CSV file")},
    out=Out(str, description="Path to normalized CSV file"),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def normalize_sku_format(context, csv_path: str) -> str:
    """Normalize SKU format: uppercase, strip whitespace, standardize prefix."""
    logger = get_dagster_logger()
    logger.info(f"Normalizing SKU format for {csv_path}")
    
    storage = FileStorage()
    df = storage.load(os.path.basename(csv_path))
    
    # Apply transformation rules
    df["sku"] = df["sku"].str.upper()
    df["sku"] = df["sku"].str.strip()
    df["sku"] = df["sku"].str.replace(r'^[A-Z]_', '', regex=True)
    
    warehouse = df["warehouse"].iloc[0]
    filename = f"{warehouse}_normalized_{datetime.now().strftime('%Y%m%d')}.csv"
    filepath = storage.save(filename, df)
    
    logger.info(f"Normalized {len(df)} records for {warehouse} warehouse")
    return filepath


@op(
    ins={
        "north_data": In(str, description="North warehouse normalized data path"),
        "south_data": In(str, description="South warehouse normalized data path"),
        "east_data": In(str, description="East warehouse normalized data path"),
        "west_data": In(str, description="West warehouse normalized data path"),
    },
    out=Out(Dict[str, Any], description="Discrepancy report data"),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def reconcile_inventory(
    context,
    north_data: str,
    south_data: str,
    east_data: str,
    west_data: str,
) -> Dict[str, Any]:
    """Reconcile inventory across all warehouses and identify discrepancies."""
    logger = get_dagster_logger()
    logger.info("Starting inventory reconciliation across all warehouses")
    
    storage = FileStorage()
    
    # Load all normalized datasets
    warehouse_data = {
        "north": storage.load(os.path.basename(north_data)),
        "south": storage.load(os.path.basename(south_data)),
        "east": storage.load(os.path.basename(east_data)),
        "west": storage.load(os.path.basename(west_data)),
    }
    
    # Combine data for cross-warehouse comparison
    combined_records = []
    for warehouse_name, df in warehouse_data.items():
        df_copy = df.copy()
        df_copy["warehouse_name"] = warehouse_name
        combined_records.append(df_copy)
    
    combined_df = pd.concat(combined_records, ignore_index=True)
    
    # Identify discrepancies by SKU
    discrepancies = []
    for sku, group in combined_df.groupby("sku"):
        quantities = group["quantity"].tolist()
        if len(set(quantities)) > 1:  # Different quantities across warehouses
            warehouse_quantities = {
                row["warehouse_name"]: int(row["quantity"])
                for _, row in group.iterrows()
            }
            discrepancies.append({
                "sku": sku,
                "warehouse_quantities": warehouse_quantities,
                "variance": max(quantities) - min(quantities),
                "mean_quantity": sum(quantities) / len(quantities),
            })
    
    logger.info(f"Identified {len(discrepancies)} SKUs with quantity discrepancies")
    
    report_data = {
        "generated_at": datetime.now().isoformat(),
        "total_skus_checked": combined_df["sku"].nunique(),
        "discrepancies_found": len(discrepancies),
        "discrepancies": discrepancies,
    }
    
    # Persist discrepancy report
    filename = f"discrepancy_report_{datetime.now().strftime('%Y%m%d')}.json"
    filepath = os.path.join(storage.base_path, filename)
    with open(filepath, 'w') as f:
        json.dump(report_data, f, indent=2)
    
    return report_data


@op(
    ins={"discrepancy_data": In(Dict[str, Any], description="Discrepancy report data")},
    out=Out(str, description="Path to generated PDF report"),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def generate_reconciliation_report(context, discrepancy_data: Dict[str, Any]) -> str:
    """Generate final PDF reconciliation report from discrepancy data."""
    logger = get_dagster_logger()
    logger.info("Generating final reconciliation report")
    
    reporter = PDFReporter()
    storage = FileStorage()
    
    output_path = os.path.join(
        storage.base_path,
        f"reconciliation_report_{datetime.now().strftime('%Y%m%d')}.pdf"
    )
    
    report_path = reporter.generate_report(discrepancy_data, output_path)
    
    logger.info(f"Reconciliation report generated at {report_path}")
    return report_path


@job(
    description="Retail inventory reconciliation pipeline with fan-out/fan-in pattern",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def retail_inventory_reconciliation_job():
    """Define the complete inventory reconciliation pipeline."""
    # Fetch phase: parallel execution across 4 warehouses
    north_fetch = fetch_warehouse_csv.configured(
        {"warehouse": "north"},
        name="fetch_north_warehouse"
    )
    south_fetch = fetch_warehouse_csv.configured(
        {"warehouse": "south"},
        name="fetch_south_warehouse"
    )
    east_fetch = fetch_warehouse_csv.configured(
        {"warehouse": "east"},
        name="fetch_east_warehouse"
    )
    west_fetch = fetch_warehouse_csv.configured(
        {"warehouse": "west"},
        name="fetch_west_warehouse"
    )
    
    # Normalize phase: parallel execution, depends on fetch completion
    north_normalized = normalize_sku_format(north_fetch)
    south_normalized = normalize_sku_format(south_fetch)
    east_normalized = normalize_sku_format(east_fetch)
    west_normalized = normalize_sku_format(west_fetch)
    
    # Reconciliation phase: fan-in from all normalizations
    discrepancy_results = reconcile_inventory(
        north_normalized,
        south_normalized,
        east_normalized,
        west_normalized,
    )
    
    # Report generation: final step
    generate_reconciliation_report(discrepancy_results)


# Schedule: daily execution starting January 1, 2024
# Note: No catchup is configured when enabling the schedule in Dagster
retail_inventory_schedule = ScheduleDefinition(
    job=retail_inventory_reconciliation_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    # start_date=datetime(2024, 1, 1),  # Uncomment if using Dagster 1.4+
)

# Launch pattern for local execution and testing
if __name__ == "__main__":
    # Execute pipeline in-process
    # For production, configure a multi-process executor with max_concurrent=4
    # to respect the maximum parallel width constraint
    result = retail_inventory_reconciliation_job.execute_in_process()
    
    if result.success:
        print("✅ Pipeline execution successful!")
        print(f"Report generated in /tmp/warehouse_inventory/")
    else:
        print("❌ Pipeline execution failed")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Failed step: {event.step_key}")
                print(f"Error: {event.event_specific_data.error}")