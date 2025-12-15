from dagster import (
    op,
    job,
    ScheduleDefinition,
    RetryPolicy,
    DefaultScheduleStatus,
    Out,
    In,
    Nothing,
    get_dagster_logger,
)
import os
import tempfile
import csv
import json
from datetime import datetime
from typing import List, Dict, Any

logger = get_dagster_logger()

# Simulated warehouse data for demonstration
WAREHOUSES = ["north", "south", "east", "west"]

# Resource stubs (if needed, but for this simple case, we can use config or direct parameters)
# I'll use op config for warehouse names and file paths

@op(
    out=Out(str, description="Path to fetched CSV file"),
    config_schema={"warehouse": str, "output_dir": str},
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 5 minute delay
)
def fetch_warehouse_csv(context) -> str:
    """Fetch CSV file from a warehouse location."""
    warehouse = context.op_config["warehouse"]
    output_dir = context.op_config["output_dir"]
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Simulate fetching CSV file
    file_path = os.path.join(output_dir, f"{warehouse}_inventory.csv")
    
    # Mock data generation for demonstration
    # In real scenario, this would fetch from actual warehouse system
    mock_data = [
        ["sku", "quantity", "warehouse"],
        [f"WH-{warehouse.upper()}-001", str(100 + hash(warehouse) % 50), warehouse],
        [f"WH-{warehouse.upper()}-002", str(200 + hash(warehouse) % 30), warehouse],
        [f"WH-{warehouse.upper()}-003", str(150 + hash(warehouse) % 40), warehouse],
        # Add more mock SKUs to reach 1000+ as mentioned
    ]
    
    # Generate more SKUs to simulate 1000+ records
    for i in range(4, 1001):
        sku = f"WH-{warehouse.upper()}-{i:04d}"
        quantity = str(50 + (hash(f"{warehouse}{i}") % 200))
        mock_data.append([sku, quantity, warehouse])
    
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(mock_data)
    
    logger.info(f"Fetched CSV for {warehouse} warehouse: {file_path}")
    return file_path


@op(
    ins={"csv_path": In(str, description="Path to warehouse CSV file")},
    out=Out(str, description="Path to normalized CSV file"),
    config_schema={"output_dir": str},
)
def normalize_sku_format(context, csv_path: str) -> str:
    """Normalize SKU format: uppercase, strip whitespace, standardize prefix."""
    output_dir = context.op_config["output_dir"]
    os.makedirs(output_dir, exist_ok=True)
    
    # Read input CSV
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # Normalize SKUs
    normalized_rows = []
    for row in rows:
        sku = row['sku']
        # Apply transformations: uppercase, strip whitespace, standardize prefix
        normalized_sku = sku.upper().strip()
        # Standardize prefix (remove warehouse-specific prefix and replace with STD)
        if normalized_sku.startswith(f"WH-{row['warehouse'].upper()}"):
            normalized_sku = normalized_sku.replace(f"WH-{row['warehouse'].upper()}", "STD", 1)
        
        normalized_rows.append({
            'sku': normalized_sku,
            'quantity': int(row['quantity']),
            'warehouse': row['warehouse']
        })
    
    # Write normalized CSV
    warehouse = normalized_rows[0]['warehouse'] if normalized_rows else "unknown"
    output_path = os.path.join(output_dir, f"{warehouse}_normalized.csv")
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['sku', 'quantity', 'warehouse'])
        writer.writeheader()
        writer.writerows(normalized_rows)
    
    logger.info(f"Normalized SKU format for {warehouse}: {output_path}")
    return output_path


@op(
    ins={
        "north_file": In(str, description="Path to north warehouse normalized file"),
        "south_file": In(str, description="Path to south warehouse normalized file"),
        "east_file": In(str, description="Path to east warehouse normalized file"),
        "west_file": In(str, description="Path to west warehouse normalized file"),
    },
    out=Out(str, description="Path to discrepancy report JSON file"),
    config_schema={"output_dir": str},
)
def reconcile_inventory_discrepancies(
    context, 
    north_file: str, 
    south_file: str, 
    east_file: str, 
    west_file: str
) -> str:
    """Reconcile inventory discrepancies across all warehouses."""
    output_dir = context.op_config["output_dir"]
    os.makedirs(output_dir, exist_ok=True)
    
    # Read all normalized files
    warehouse_data = {}
    for warehouse, file_path in [
        ("north", north_file),
        ("south", south_file),
        ("east", east_file),
        ("west", west_file),
    ]:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            warehouse_data[warehouse] = {row['sku']: int(row['quantity']) for row in reader}
    
    # Find discrepancies
    all_skus = set()
    for data in warehouse_data.values():
        all_skus.update(data.keys())
    
    discrepancies = []
    for sku in sorted(all_skus):
        quantities = {}
        for warehouse, data in warehouse_data.items():
            quantities[warehouse] = data.get(sku, 0)
        
        # Check if there are discrepancies (different quantities across warehouses)
        unique_quantities = set(quantities.values())
        if len(unique_quantities) > 1:
            discrepancies.append({
                "sku": sku,
                "quantities": quantities,
                "variance": max(quantities.values()) - min(quantities.values()),
                "recommendation": "Review stock levels and adjust accordingly"
            })
    
    # Generate report
    report_path = os.path.join(output_dir, "discrepancy_report.json")
    with open(report_path, 'w') as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "total_skus_checked": len(all_skus),
            "discrepancies_found": len(discrepancies),
            "discrepancies": discrepancies[:23],  # Limit to 23 as mentioned in description
            "summary": f"Found {len(discrepancies)} SKUs with quantity mismatches across warehouses"
        }, f, indent=2)
    
    logger.info(f"Reconciliation complete. Discrepancies found: {len(discrepancies)}")
    logger.info(f"Report saved to: {report_path}")
    return report_path


@op(
    ins={"discrepancy_report_path": In(str, description="Path to discrepancy report JSON")},
    out=Out(str, description="Path to generated PDF report"),
    config_schema={"output_dir": str},
)
def generate_final_reconciliation_report(context, discrepancy_report_path: str) -> str:
    """Generate comprehensive PDF reconciliation report."""
    output_dir = context.op_config["output_dir"]
    os.makedirs(output_dir, exist_ok=True)
    
    # Read discrepancy report
    with open(discrepancy_report_path, 'r') as f:
        report_data = json.load(f)
    
    # Generate PDF report (simulated as HTML for demonstration)
    # In real scenario, use reportlab or similar library
    pdf_path = os.path.join(output_dir, "reconciliation_report.pdf")
    
    # Create a simple HTML report as PDF simulation
    html_content = f"""
    <html>
    <head><title>Inventory Reconciliation Report</title></head>
    <body>
        <h1>Inventory Reconciliation Report</h1>
        <p>Generated: {report_data['generated_at']}</p>
        <p>Total SKUs Checked: {report_data['total_skus_checked']}</p>
        <p>Discrepancies Found: {report_data['discrepancies_found']}</p>
        
        <h2>Discrepancy Details (showing first 23)</h2>
        <table border="1">
            <tr>
                <th>SKU</th>
                <th>North</th>
                <th>South</th>
                <th>East</th>
                <th>West</th>
                <th>Variance</th>
                <th>Recommendation</th>
            </tr>
    """
    
    for disc in report_data['discrepancies']:
        html_content += f"""
            <tr>
                <td>{disc['sku']}</td>
                <td>{disc['quantities']['north']}</td>
                <td>{disc['quantities']['south']}</td>
                <td>{disc['quantities']['east']}</td>
                <td>{disc['quantities']['west']}</td>
                <td>{disc['variance']}</td>
                <td>{disc['recommendation']}</td>
            </tr>
        """
    
    html_content += """
        </table>
    </body>
    </html>
    """
    
    # Write as PDF (simulated as HTML with .pdf extension for demo)
    with open(pdf_path, 'w') as f:
        f.write(html_content)
    
    logger.info(f"Final reconciliation report generated: {pdf_path}")
    return pdf_path


@job(
    description="Retail inventory reconciliation pipeline with fan-out/fan-in pattern",
    tags={"pipeline_type": "inventory_reconciliation", "parallel_width": "4"},
)
def retail_inventory_reconciliation_job():
    """Define the retail inventory reconciliation pipeline."""
    
    # Fan-out: Fetch CSV files from all warehouses in parallel
    fetch_ops = []
    for warehouse in WAREHOUSES:
        fetch_op = fetch_warehouse_csv.configured(
            {
                "warehouse": warehouse,
                "output_dir": f"/tmp/dagster/warehouse_raw/{warehouse}",
            },
            name=f"fetch_{warehouse}_csv",
        )
        fetch_ops.append(fetch_op())
    
    # Fan-out: Normalize SKU formats in parallel
    normalize_ops = []
    for i, warehouse in enumerate(WAREHOUSES):
        normalize_op = normalize_sku_format.configured(
            {
                "output_dir": f"/tmp/dagster/warehouse_normalized/{warehouse}",
            },
            name=f"normalize_{warehouse}_sku",
        )
        # Each normalization depends on its corresponding fetch
        normalize_ops.append(normalize_op(fetch_ops[i]))
    
    # Fan-in: Reconciliation depends on all normalizations
    reconcile_op = reconcile_inventory_discrepancies.configured(
        {
            "output_dir": "/tmp/dagster/reconciliation",
        },
        name="reconcile_inventory",
    )
    
    # Generate final report depends on reconciliation
    report_op = generate_final_reconciliation_report.configured(
        {
            "output_dir": "/tmp/dagster/reports",
        },
        name="generate_pdf_report",
    )
    
    # Connect the graph
    reconcile_result = reconcile_op(
        north_file=normalize_ops[0],
        south_file=normalize_ops[1],
        east_file=normalize_ops[2],
        west_file=normalize_ops[3],
    )
    
    report_op(reconcile_result)


# Schedule definition
retail_inventory_reconciliation_schedule = ScheduleDefinition(
    job=retail_inventory_reconciliation_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,  # Start date controlled by user
    tags={"schedule_type": "daily_inventory_reconciliation"},
)


if __name__ == "__main__":
    # Execute the job in process for testing
    result = retail_inventory_reconciliation_job.execute_in_process(
        run_config={
            "execution": {
                "config": {
                    "multiprocess": {
                        "max_concurrent": 4,  # Maximum parallel width: 4
                    }
                }
            }
        }
    )
    
    if result.success:
        logger.info("Pipeline execution completed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        for event in result.all_node_events:
            if event.event_type_value == "STEP_FAILURE":
                logger.error(f"Step failed: {event.step_key}")