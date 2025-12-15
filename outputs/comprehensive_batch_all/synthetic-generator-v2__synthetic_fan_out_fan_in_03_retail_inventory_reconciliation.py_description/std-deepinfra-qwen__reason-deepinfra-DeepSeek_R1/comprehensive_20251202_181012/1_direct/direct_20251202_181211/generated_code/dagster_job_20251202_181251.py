from dagster import op, job, RetryPolicy, In, Out, Field, String, graph

# Resources and Configurations
# Example resource for fetching CSV files from warehouses
# class WarehouseCSVFetcher:
#     def fetch_csv(self, location: str) -> str:
#         # Simulate fetching CSV file from warehouse
#         return f"csv_data_{location}.csv"

# Example resource for generating PDF reports
# class PDFReportGenerator:
#     def generate_pdf_report(self, discrepancy_report: str) -> str:
#         # Simulate generating PDF report
#         return f"reconciliation_report.pdf"

# Ops
@op(
    config_schema={"location": Field(String, is_required=True)},
    out=Out(String, description="Path to the fetched CSV file"),
)
def fetch_warehouse_csv(context):
    """Fetch CSV file from a specific warehouse location."""
    location = context.op_config["location"]
    # Simulate fetching CSV file
    csv_path = f"csv_data_{location}.csv"
    context.log.info(f"Fetched CSV file from {location}: {csv_path}")
    return csv_path

@op(
    ins={"csv_path": In(String, description="Path to the fetched CSV file")},
    out=Out(String, description="Path to the normalized CSV file"),
)
def normalize_sku_format(context, csv_path):
    """Normalize SKU formats in the CSV file."""
    # Simulate normalization process
    normalized_csv_path = f"normalized_{csv_path}"
    context.log.info(f"Normalized SKU formats in {csv_path} to {normalized_csv_path}")
    return normalized_csv_path

@op(
    ins={
        "north_csv": In(String, description="Normalized CSV file from North warehouse"),
        "south_csv": In(String, description="Normalized CSV file from South warehouse"),
        "east_csv": In(String, description="Normalized CSV file from East warehouse"),
        "west_csv": In(String, description="Normalized CSV file from West warehouse"),
    },
    out=Out(String, description="Path to the discrepancy report file"),
)
def reconcile_inventory(context, north_csv, south_csv, east_csv, west_csv):
    """Reconcile inventory discrepancies across all warehouses."""
    # Simulate reconciliation process
    discrepancy_report_path = "discrepancy_report.csv"
    context.log.info(f"Reconciled inventory discrepancies and generated report: {discrepancy_report_path}")
    return discrepancy_report_path

@op(
    ins={"discrepancy_report_path": In(String, description="Path to the discrepancy report file")},
    out=Out(String, description="Path to the final reconciliation report PDF"),
)
def generate_reconciliation_report(context, discrepancy_report_path):
    """Generate the final reconciliation report PDF."""
    # Simulate report generation
    report_pdf_path = "reconciliation_report.pdf"
    context.log.info(f"Generated final reconciliation report: {report_pdf_path}")
    return report_pdf_path

# Job
@job(
    resource_defs={
        # "warehouse_csv_fetcher": WarehouseCSVFetcher(),
        # "pdf_report_generator": PDFReportGenerator(),
    },
    config={
        "ops": {
            "fetch_warehouse_csv": {
                "config": {
                    "location": {"north": "north", "south": "south", "east": "east", "west": "west"}
                }
            }
        }
    },
    tags={"dagster/pipeline/schedule": "daily", "dagster/pipeline/start_date": "2024-01-01"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Retail inventory reconciliation pipeline",
)
def retail_inventory_reconciliation():
    north_csv = fetch_warehouse_csv.alias("fetch_north_csv")()
    south_csv = fetch_warehouse_csv.alias("fetch_south_csv")()
    east_csv = fetch_warehouse_csv.alias("fetch_east_csv")()
    west_csv = fetch_warehouse_csv.alias("fetch_west_csv")()

    north_normalized_csv = normalize_sku_format.alias("normalize_north_sku")(csv_path=north_csv)
    south_normalized_csv = normalize_sku_format.alias("normalize_south_sku")(csv_path=south_csv)
    east_normalized_csv = normalize_sku_format.alias("normalize_east_sku")(csv_path=east_csv)
    west_normalized_csv = normalize_sku_format.alias("normalize_west_sku")(csv_path=west_csv)

    discrepancy_report = reconcile_inventory(
        north_csv=north_normalized_csv,
        south_csv=south_normalized_csv,
        east_csv=east_normalized_csv,
        west_csv=west_normalized_csv,
    )

    final_report = generate_reconciliation_report(discrepancy_report_path=discrepancy_report)

if __name__ == "__main__":
    result = retail_inventory_reconciliation.execute_in_process()