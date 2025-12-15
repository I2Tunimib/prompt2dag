from dagster import op, job, RetryPolicy, In, Out, Output, Field, String, Nothing

# Resource stubs
class WarehouseClient:
    def fetch_csv(self, location: str) -> str:
        """Simulate fetching CSV file from warehouse."""
        return f"mock_csv_{location}.csv"

class ReportGenerator:
    def generate_pdf_report(self, discrepancy_report: str) -> str:
        """Simulate generating PDF report from discrepancy report."""
        return "final_reconciliation_report.pdf"

# Ops
@op(
    required_resource_keys={"warehouse_client"},
    out=Out(dagster_type=str, description="Path to fetched CSV file"),
)
def fetch_warehouse_csv(context, location: str) -> str:
    """Fetch CSV file from a specific warehouse location."""
    csv_path = context.resources.warehouse_client.fetch_csv(location)
    context.log.info(f"Fetched CSV from {location}: {csv_path}")
    return csv_path

@op(
    out=Out(dagster_type=str, description="Path to normalized CSV file"),
)
def normalize_sku_format(context, csv_path: str) -> str:
    """Normalize SKU formats in the CSV file."""
    normalized_csv_path = f"normalized_{csv_path}"
    context.log.info(f"Normalized SKU formats in {csv_path} to {normalized_csv_path}")
    return normalized_csv_path

@op(
    out=Out(dagster_type=str, description="Path to discrepancy report"),
)
def reconcile_inventory(context, *normalized_csv_paths: str) -> str:
    """Reconcile inventory discrepancies across normalized CSV files."""
    discrepancy_report_path = "discrepancy_report.csv"
    context.log.info(f"Reconciled inventory from {normalized_csv_paths} to {discrepancy_report_path}")
    return discrepancy_report_path

@op(
    required_resource_keys={"report_generator"},
    out=Out(dagster_type=str, description="Path to final reconciliation report"),
)
def generate_reconciliation_report(context, discrepancy_report: str) -> str:
    """Generate the final reconciliation report in PDF format."""
    final_report_path = context.resources.report_generator.generate_pdf_report(discrepancy_report)
    context.log.info(f"Generated final reconciliation report: {final_report_path}")
    return final_report_path

# Job
@job(
    resource_defs={
        "warehouse_client": Field(WarehouseClient, is_required=True),
        "report_generator": Field(ReportGenerator, is_required=True),
    },
    tags={"dagster/priority": "1"},
    description="Retail inventory reconciliation pipeline",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def retail_inventory_reconciliation():
    north_csv = fetch_warehouse_csv.alias("fetch_north_csv")(location="north")
    south_csv = fetch_warehouse_csv.alias("fetch_south_csv")(location="south")
    east_csv = fetch_warehouse_csv.alias("fetch_east_csv")(location="east")
    west_csv = fetch_warehouse_csv.alias("fetch_west_csv")(location="west")

    north_normalized = normalize_sku_format.alias("normalize_north_sku")(csv_path=north_csv)
    south_normalized = normalize_sku_format.alias("normalize_south_sku")(csv_path=south_csv)
    east_normalized = normalize_sku_format.alias("normalize_east_sku")(csv_path=east_csv)
    west_normalized = normalize_sku_format.alias("normalize_west_sku")(csv_path=west_csv)

    discrepancy_report = reconcile_inventory(
        north_normalized, south_normalized, east_normalized, west_normalized
    )

    final_report = generate_reconciliation_report(discrepancy_report=discrepancy_report)

if __name__ == "__main__":
    result = retail_inventory_reconciliation.execute_in_process()