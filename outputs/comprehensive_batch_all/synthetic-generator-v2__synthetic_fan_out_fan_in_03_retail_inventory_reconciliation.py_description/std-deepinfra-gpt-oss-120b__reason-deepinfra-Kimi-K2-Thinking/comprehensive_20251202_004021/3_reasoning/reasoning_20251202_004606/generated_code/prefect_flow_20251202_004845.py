import csv
from pathlib import Path
from typing import List, Dict, Any
from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def fetch_warehouse_csv(warehouse: str) -> str:
    """Fetch CSV inventory file for a specific warehouse."""
    # Mock: Create sample inventory data with intentional discrepancies
    base_skus = ["STD-001", "STD-002", "STD-003", "STD-004"]
    quantity_offsets = {"north": 0, "south": 10, "east": 0, "west": -5}
    
    sample_data = []
    for i, sku in enumerate(base_skus):
        base_qty = 100 + (i * 50)
        adjusted_qty = base_qty + quantity_offsets[warehouse]
        sample_data.append({
            "sku": sku,
            "quantity": adjusted_qty,
            "location": warehouse
        })
    
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = output_dir / f"{warehouse}_inventory.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["sku", "quantity", "location"])
        writer.writeheader()
        writer.writerows(sample_data)
    
    return str(file_path)


@task(retries=2, retry_delay_seconds=300)
def normalize_sku_format(csv_path: str, warehouse: str) -> str:
    """Normalize SKU format: uppercase, strip whitespace, standardize prefix."""
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        data = list(reader)
    
    normalized_rows = []
    for row in data:
        sku = row["sku"].upper().strip()
        if not sku.startswith("STD"):
            sku = f"STD-{sku.split('-')[-1]}"
        
        normalized_rows.append({
            "sku": sku,
            "quantity": int(row["quantity"]),
            "location": warehouse
        })
    
    output_dir = Path("data/normalized")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    normalized_path = output_dir / f"{warehouse}_normalized.csv"
    with open(normalized_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["sku", "quantity", "location"])
        writer.writeheader()
        writer.writerows(normalized_rows)
    
    return str(normalized_path)


@task(retries=2, retry_delay_seconds=300)
def reconcile_inventory(
    north_file: str, south_file: str, east_file: str, west_file: str
) -> str:
    """Reconcile inventory across warehouses and generate discrepancy report."""
    warehouse_data = {}
    for location, file_path in zip(
        ["north", "south", "east", "west"], 
        [north_file, south_file, east_file, west_file]
    ):
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            warehouse_data[location] = {row["sku"]: int(row["quantity"]) for row in reader}
    
    all_skus = set()
    for data in warehouse_data.values():
        all_skus.update(data.keys())
    
    discrepancies = []
    for sku in sorted(all_skus):
        quantities = {
            loc: warehouse_data[loc].get(sku, 0) 
            for loc in ["north", "south", "east", "west"]
        }
        
        unique_qtys = set(quantities.values())
        if len(unique_qtys