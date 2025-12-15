from dagster import op, job, RetryPolicy, In, Out, Output, Field, String, Int, Nothing

# Resources and Configurations
# Example resource for fetching CSV files from warehouse systems
class WarehouseCSVFetcher:
    def fetch_csv(self, location: str) -> str:
        """Simulate fetching CSV file from a warehouse."""
        return f"mock_csv_{location}.csv"

# Example resource for generating PDF reports
class PDFReportGenerator:
    def generate_pdf(self, discrepancy_report: str) -> str:
        """Simulate generating a PDF report from a discrepancy report."""
        return f"reconciliation_report.pdf"

# Ops
@op(
    config_schema={
        "location": Field(String, description="The warehouse location (north, south, east, west).")
    },
    out=Out(String, description="Path to the fetched CSV file."),
)
def fetch_warehouse_csv(context) -> str:
    """Fetch CSV file from a warehouse."""
    location = context.op_config["location"]
    fetcher = WarehouseCSVFetcher()
    csv_path = fetcher.fetch_csv(location)
    return csv_path

@op(
    ins={"csv_path": In(String, description="Path to the CSV file to normalize.")},
    out=Out(String, description="Path to the normalized CSV file."),
)
def normalize_sku_format(context, csv_path: str) -> str:
    """Normalize SKU formats in the CSV file."""
    # Simulate normalization process
    normalized_csv_path = f"normalized_{csv_path}"
    return normalized_csv_path

@op(
    ins={
        "north_csv": In(String, description="Normalized CSV from the north warehouse."),
        "south_csv": In(String, description="Normalized CSV from the south warehouse."),
        "east_csv": In(String, description="Normalized CSV from the east warehouse."),
        "west_csv": In(String, description="Normalized CSV from the west warehouse."),
    },
    out=Out(String, description="Path to the discrepancy report file."),
)
def reconcile_inventory(context, north_csv: str, south_csv: str, east_csv: str, west_csv: str) -> str:
    """Reconcile inventory discrepancies across all warehouses."""
    # Simulate reconciliation process
    discrepancy_report_path = "discrepancy_report.csv"
    return discrepancy_report_path

@op(
    ins={"discrepancy_report_path": In(String, description="Path to the discrepancy report file.")},
    out=Out(String, description="Path to the final reconciliation report PDF."),
)
def generate_reconciliation_report(context, discrepancy_report_path: str) -> str:
    """Generate the final reconciliation report PDF."""
    generator = PDFReportGenerator()
    pdf_report_path = generator.generate_pdf(discrepancy_report_path)
    return pdf_report_path

# Job
@job(
    resource_defs={},
    config={
        "ops": {
            "fetch_warehouse_csv": [
                {"config": {"location": "north"}},
                {"config": {"location": "south"}},
                {"config": {"location": "east"}},
                {"config": {"location": "west"}},
            ]
        }
    },
    tags={
        "dagster/pipeline/schedule": "daily",
        "dagster/pipeline/start_date": "2024-01-01",
        "dagster/pipeline/max_retries": 2,
        "dagster/pipeline/retry_delay": 300,
        "dagster/pipeline/catchup": False,
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def retail_inventory_reconciliation():
    north_csv = fetch_warehouse_csv.alias("fetch_north_csv")()
    south_csv = fetch_warehouse_csv.alias("fetch_south_csv")()
    east_csv = fetch_warehouse_csv.alias("fetch_east_csv")()
    west_csv = fetch_warehouse_csv.alias("fetch_west_csv")()

    normalized_north_csv = normalize_sku_format.alias("normalize_north_sku")(csv_path=north_csv)
    normalized_south_csv = normalize_sku_format.alias("normalize_south_sku")(csv_path=south_csv)
    normalized_east_csv = normalize_sku_format.alias("normalize_east_sku")(csv_path=east_csv)
    normalized_west_csv = normalize_sku_format.alias("normalize_west_sku")(csv_path=west_csv)

    discrepancy_report = reconcile_inventory(
        north_csv=normalized_north_csv,
        south_csv=normalized_south_csv,
        east_csv=normalized_east_csv,
        west_csv=normalized_west_csv,
    )

    generate_reconciliation_report(discrepancy_report_path=discrepancy_report)

# Launch pattern
if __name__ == "__main__":
    result = retail_inventory_reconciliation.execute_in_process()