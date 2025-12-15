from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from typing import List, Dict, Any
import os
import csv
import json
from datetime import datetime
import hashlib

# Mock functions to simulate real operations
def mock_fetch_csv(warehouse: str) -> str:
    """Simulate fetching CSV from warehouse."""
    # In real implementation, this would fetch from actual warehouse system
    filename = f"/tmp/{warehouse}_inventory_{datetime.now().strftime('%Y%m%d')}.csv"
    # Create mock CSV data
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['sku', 'quantity'])
        # Generate mock SKUs with warehouse-specific formats
        for i in range(1000):
            if warehouse == 'north':
                sku = f"N-{i:04d}-A"
            elif warehouse == 'south':
                sku = f"S_SKU_{i}"
            elif warehouse == 'east':
                sku = f"EAST-{i}"
            else:  # west
                sku = f"W{i:05d}"
            writer.writerow([sku, 100 + i % 50])
    return filename

def mock_normalize_sku(csv_path: str, warehouse: str) -> str:
    """Simulate SKU normalization."""
    # In real implementation, this would apply actual transformation rules
    normalized_path = csv_path.replace('.csv', '_normalized.csv')
    with open(csv_path, 'r') as infile, open(normalized_path, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)
        writer.writerow(['standardized_sku', 'quantity', 'original_warehouse'])
        
        for row in reader:
            sku = row['sku']
            # Apply transformation rules
            sku = sku.upper().strip()
            # Standardize prefixes
            if sku.startswith('N-'):
                sku = sku.replace('N-', 'WH-N-')
            elif sku.startswith('S_SKU_'):
                sku = sku.replace('S_SKU_', 'WH-S-')
            elif sku.startswith('EAST-'):
                sku = sku.replace('EAST-', 'WH-E-')
            elif sku.startswith('W'):
                sku = 'WH-W-' + sku[1:].lstrip('0')
            
            writer.writerow([sku, row['quantity'], warehouse])
    
    return normalized_path

def mock_reconcile_inventory(normalized_paths: List[str]) -> tuple:
    """Simulate inventory reconciliation."""
    # In real implementation, this would compare actual inventory data
    warehouse_data = {}
    all_skus = set()
    
    # Read all normalized files
    for path in normalized_paths:
        warehouse = os.path.basename(path).split('_')[0]
        warehouse_data[warehouse] = {}
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sku = row['standardized_sku']
                quantity = int(row['quantity'])
                warehouse_data[warehouse][sku] = quantity
                all_skus.add(sku)
    
    # Find discrepancies
    discrepancies = []
    for sku in sorted(all_skus):
        quantities = []
        for warehouse, data in warehouse_data.items():
            qty = data.get(sku, 0)
            quantities.append({'warehouse': warehouse, 'quantity': qty})
        
        # Check if there are mismatches
        unique_qtys = set(q['quantity'] for q in quantities)
        if len(unique_qtys) > 1:
            discrepancies.append({
                'sku': sku,
                'quantities': quantities,
                'variance': max(unique_qtys) - min(unique_qtys)
            })
    
    # Generate discrepancy report
    report_path = "/tmp/discrepancy_report.json"
    with open(report_path, 'w') as f:
        json.dump({
            'total_skus_checked': len(all_skus),
            'discrepancies_found': len(discrepancies),
            'discrepancies': discrepancies,
            'generated_at': datetime.now().isoformat()
        }, f, indent=2)
    
    return discrepancies, report_path

def mock_generate_pdf(discrepancy_data: List[Dict], discrepancy_report_path: str) -> str:
    """Simulate PDF report generation."""
    # In real implementation, this would generate an actual PDF
    pdf_path = "/tmp/reconciliation_report.pdf"
    # Create a simple text-based "PDF" for demonstration
    with open(pdf_path, 'w') as f:
        f.write("INVENTORY RECONCILIATION REPORT\n")
        f.write("=" * 40 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Discrepancies Found: {len(discrepancy_data)}\n\n")
        
        for disc in discrepancy_data[:10]:  # Show first 10 for brevity
            f.write(f"SKU: {disc['sku']}\n")
            f.write(f"Variance: {disc['variance']} units\n")
            f.write("Quantities by Warehouse:\n")
            for q in disc['quantities']:
                f.write(f"  {q['warehouse']}: {q['quantity']}\n")
            f.write("-" * 30 + "\n")
        
        if len(discrepancy_data) > 10:
            f.write(f"\n... and {len(discrepancy_data) - 10} more discrepancies\n")
        
        f.write("\nRECOMMENDATIONS:\n")
        f.write("- Review high-variance SKUs immediately\n")
        f.write("- Conduct physical count for discrepant items\n")
        f.write("- Update inventory records in all systems\n")
    
    return pdf_path

# Prefect Tasks
@task(retries=2, retry_delay_seconds=300)
def fetch_warehouse_csv(warehouse: str) -> str:
    """Fetch inventory CSV file from specified warehouse."""
    return mock_fetch_csv(warehouse)

@task(retries=2, retry_delay_seconds=300)
def normalize_sku_format(csv_path: str, warehouse: str) -> str:
    """Normalize SKU formats for a warehouse's inventory data."""
    return mock_normalize_sku(csv_path, warehouse)

@task(retries=2, retry_delay_seconds=300)
def reconcile_inventory(normalized_paths: List[str]) -> tuple:
    """Reconcile inventory across all warehouses and identify discrepancies."""
    return mock_reconcile_inventory(normalized_paths)

@task(retries=2, retry_delay_seconds=300)
def generate_pdf_report(discrepancy_data: List[Dict], discrepancy_report_path: str) -> str:
    """Generate comprehensive PDF reconciliation report."""
    return mock_generate_pdf(discrepancy_data, discrepancy_report_path)

# Prefect Flow
@flow(name="retail-inventory-reconciliation")
def retail_inventory_reconciliation():
    """
    Daily retail inventory reconciliation pipeline.
    Fetches data from 4 warehouses, normalizes SKUs, reconciles discrepancies,
    and generates a PDF report.
    """
    warehouses = ['north', 'south', 'east', 'west']
    
    # Stage 1: Fetch CSV files in parallel
    fetch_tasks = [
        fetch_warehouse_csv.submit(warehouse) 
        for warehouse in warehouses
    ]
    
    # Stage 2: Normalize SKU formats in parallel
    # Wait for all fetch tasks to complete and pass results to normalization
    normalize_tasks = [
        normalize_sku_format.submit(fetch_task.result(), warehouse)
        for warehouse, fetch_task in zip(warehouses, fetch_tasks)
    ]
    
    # Stage 3: Reconcile inventory (sequential, after all normalizations)
    # Wait for all normalize tasks to complete
    normalized_paths = [task.result() for task in normalize_tasks]
    discrepancy_data, discrepancy_report_path = reconcile_inventory(normalized_paths)
    
    # Stage 4: Generate final report (sequential, after reconciliation)
    pdf_path = generate_pdf_report(discrepancy_data, discrepancy_report_path)
    
    return {
        "discrepancy_count": len(discrepancy_data),
        "discrepancy_report": discrepancy_report_path,
        "final_pdf_report": pdf_path
    }

if __name__ == '__main__':
    # For local execution
    # For scheduled deployment, use:
    # prefect deployment build retail_inventory_reconciliation.py:retail_inventory_reconciliation \
    #   --name "daily-inventory-reconciliation" \
    #   --schedule "0 2 * * *" \
    #   --start-date "2024-01-01" \
    #   --apply
    retail_inventory_reconciliation()