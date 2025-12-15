from prefect import flow, task
from prefect.futures import PrefectFuture
from typing import List, Dict
import time

@task(retries=2, retry_delay_seconds=300)
def fetch_warehouse_data(warehouse_name: str) -> str:
    """Mock fetching CSV data from specified warehouse system"""
    print(f"Fetching data from {warehouse_name} warehouse...")
    time.sleep(1)  # Simulate network call
    return f"/mock/path/{warehouse_name}_inventory.csv"

@task(retries=2, retry_delay_seconds=300)
def normalize_sku_formats(input_file: str) -> List[Dict]:
    """Normalize SKU formats with transformation rules"""
    print(f"Normalizing {input_file}...")
    # Simulate transformation: uppercase, strip whitespace, add prefix
    time.sleep(1.5)
    return [{"sku": "STD-1234", "quantity": 100}]  # Mock normalized data

@task
def reconcile_inventory_discrepancies(normalized_data: List[List[Dict]]) -> str:
    """Identify and report inventory discrepancies across warehouses"""
    print("Reconciling inventory data...")
    # Mock comparison logic
    discrepancies = [{"sku": "STD-1234", "variances": {"north": 100, "south": 95}}]
    report_path = "/mock/reports/discrepancy_report.json"
    print(f"Generated discrepancy report at {report_path}")
    return report_path

@task
def generate_final_report(report_data: str) -> None:
    """Generate comprehensive PDF reconciliation report"""
    print(f"Generating final report from {report_data}...")
    # Simulate PDF generation
    time.sleep(2)
    print("Final reconciliation report created at /mock/reports/final_report.pdf")

@flow(
    name="Retail Inventory Reconciliation Pipeline",
    description="Daily inventory reconciliation across four warehouses",
    retries=2,
    retry_delay_seconds=300
)
def inventory_reconciliation_flow():
    # Fetch data from all warehouses in parallel
    warehouses = ["north", "south", "east", "west"]
    fetch_futures = [fetch_warehouse_data.submit(w) for w in warehouses]

    # Normalize all datasets concurrently
    normalize_futures = [normalize_sku_formats.submit(future.result()) for future in fetch_futures]

    # Process reconciliation after all normalizations complete
    normalized_data = [future.result() for future in normalize_futures]
    discrepancy_report = reconcile_inventory_discrepancies(normalized_data)

    # Generate final output
    generate_final_report(discrepancy_report)

    # Comment for deployment configuration:
    # Schedule: cron "0 2 * * *" (daily at 2am UTC)
    # Start date: 2024-01-01, no catchup

if __name__ == "__main__":
    inventory_reconciliation_flow()