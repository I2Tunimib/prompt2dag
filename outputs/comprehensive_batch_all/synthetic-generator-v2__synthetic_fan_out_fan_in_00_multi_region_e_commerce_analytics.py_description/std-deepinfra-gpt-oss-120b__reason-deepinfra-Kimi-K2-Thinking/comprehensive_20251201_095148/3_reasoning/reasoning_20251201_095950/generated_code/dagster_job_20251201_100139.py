from dagster import (
    op,
    job,
    Out,
    In,
    RetryPolicy,
    ScheduleDefinition,
    Definitions,
    ResourceDefinition,
    get_dagster_logger,
    OpExecutionContext,
)
from typing import List, Dict, Any
import csv
import json
from datetime import datetime
import os

logger = get_dagster_logger()

# Resource stubs for external dependencies
class CSVDataResource:
    def __init__(self, base_path: str):
        self.base_path = base_path
    
    def read_csv(self, region: str) -> List[Dict[str, Any]]:
        """Read sales data from CSV for a region."""
        file_path = os.path.join(self.base_path, f"{region.lower()}_sales.csv")
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path}, returning empty list")
            return []

class ExchangeRateResource:
    def __init__(self, rates: Dict[str, float]):
        # In production, this would fetch from an API
        self.rates = rates
    
    def get_rate(self, from_currency: str, to_currency: str) -> float:
        """Get exchange rate from one currency to another."""
        key = f"{from_currency}_{to_currency}"
        return self.rates.get(key, 1.0)

# Resources configuration
resources = {
    "csv_data": ResourceDefinition.hardcoded_resource(
        CSVDataResource(base_path="/data/sales")
    ),
    "exchange_rates": ResourceDefinition.hardcoded_resource(
        ExchangeRateResource(rates={"EUR_USD": 1.1, "JPY_USD": 0.007})
    ),
    # Email resource stub
    "email_notifier": ResourceDefinition.hardcoded_resource(
        {"smtp_host": "smtp.example.com", "from_email": "dagster@example.com"}
    ),
}

# Ops
@op(
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def start_pipeline(context: OpExecutionContext):
    """Marks pipeline initiation."""
    logger.info("Starting multi-region ecommerce analytics pipeline")
    return None

@op(
    out=Out(List[Dict[str, Any]], description="US-East sales data"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    required_resource_keys={"csv_data"},
)
def ingest_us_east(context: OpExecutionContext) -> List[Dict[str, Any]]:
    """Ingest US-East region sales data from CSV source."""
    logger.info("Ingesting US-East region sales data")
    data = context.resources.csv_data.read_csv("us_east")
    logger.info(f"Loaded {len(data)} records from US-East")
    return data

@op(
    out=Out(List[Dict[str, Any]], description="US-West sales data"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    required_resource_keys={"csv_data"},
)
def ingest_us_west(context: OpExecutionContext) -> List[Dict[str, Any]]:
    """Ingest US-West region sales data from CSV source."""
    logger.info("Ingesting US-West region sales data")
    data = context.resources.csv_data.read_csv("us_west")
    logger.info(f"Loaded {len(data)} records from US-West")
    return data

@op(
    out=Out(List[Dict[str, Any]], description="EU sales data"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    required_resource_keys={"csv_data"},
)
def ingest_eu(context: OpExecutionContext) -> List[Dict[str, Any]]:
    """Ingest EU region sales data from CSV source."""
    logger.info("Ingesting EU region sales data")
    data = context.resources.csv_data.read_csv("eu")
    logger.info(f"Loaded {len(data)} records from EU")
    return data

@op(
    out=Out(List[Dict[str, Any]], description="APAC sales data"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    required_resource_keys={"csv_data"},
)
def ingest_apac(context: OpExecutionContext) -> List[Dict[str, Any]]:
    """Ingest APAC region sales data from CSV source."""
    logger.info("Ingesting APAC region sales data")
    data = context.resources.csv_data.read_csv("apac")
    logger.info(f"Loaded {len(data)} records from APAC")
    return data

@op(
    ins={"data": In(List[Dict[str, Any]], description="US-East data in USD")},
    out=Out(List[Dict[str, Any]], description="US-East data in USD"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_us_east(context: OpExecutionContext, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert US-East data (already USD, no conversion needed)."""
    logger.info("Converting US-East data (pass-through)")
    # Add currency metadata
    for record in data:
        record['currency'] = 'USD'
        record['converted'] = True
    return data

@op(
    ins={"data": In(List[Dict[str, Any]], description="US-West data in USD")},
    out=Out(List[Dict[str, Any]], description="US-West data in USD"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_us_west(context: OpExecutionContext, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert US-West data (already USD, no conversion needed)."""
    logger.info("Converting US-West data (pass-through)")
    # Add currency metadata
    for record in data:
        record['currency'] = 'USD'
        record['converted'] = True
    return data

@op(
    ins={"data": In(List[Dict[str, Any]], description="EU data in EUR")},
    out=Out(List[Dict[str, Any]], description="EU data converted to USD"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    required_resource_keys={"exchange_rates"},
)
def convert_eu(context: OpExecutionContext, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert EU data from EUR to USD using daily exchange rates."""
    logger.info("Converting EU data from EUR to USD")
    rate = context.resources.exchange_rates.get_rate("EUR", "USD")
    logger.info(f"Using EUR to USD rate: {rate}")
    
    converted_data = []
    for record in data:
        converted = dict(record)
        try:
            # Assuming 'amount' field exists
            original_amount = float(converted.get('amount', 0))
            converted['original_amount'] = original_amount
            converted['amount'] = original_amount * rate
            converted['original_currency'] = 'EUR'
            converted['currency'] = 'USD'
            converted['exchange_rate'] = rate
            converted['converted'] = True
        except (ValueError, TypeError):
            logger.warning(f"Could not convert amount for record: {record}")
            converted['amount'] = 0
            converted['converted'] = False
        converted_data.append(converted)
    
    return converted_data

@op(
    ins={"data": In(List[Dict[str, Any]], description="APAC data in JPY")},
    out=Out(List[Dict[str, Any]], description="APAC data converted to USD"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    required_resource_keys={"exchange_rates"},
)
def convert_apac(context: OpExecutionContext, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert APAC data from JPY to USD using daily exchange rates."""
    logger.info("Converting APAC data from JPY to USD")
    rate = context.resources.exchange_rates.get_rate("JPY", "USD")
    logger.info(f"Using JPY to USD rate: {rate}")
    
    converted_data = []
    for record in data:
        converted = dict(record)
        try:
            # Assuming 'amount' field exists
            original_amount = float(converted.get('amount', 0))
            converted['original_amount'] = original_amount
            converted['amount'] = original_amount * rate
            converted['original_currency'] = 'JPY'
            converted['currency'] = 'USD'
            converted['exchange_rate'] = rate
            converted['converted'] = True
        except (ValueError, TypeError):
            logger.warning(f"Could not convert amount for record: {record}")
            converted['amount'] = 0
            converted['converted'] = False
        converted_data.append(converted)
    
    return converted_data

@op(
    ins={
        "us_east_data": In(List[Dict[str, Any]], description="Converted US-East data"),
        "us_west_data": In(List[Dict[str, Any]], description="Converted US-West data"),
        "eu_data": In(List[Dict[str, Any]], description="Converted EU data"),
        "apac_data": In(List[Dict[str, Any]], description="Converted APAC data"),
    },
    out=Out(str, description="Path to generated global revenue report"),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    required_resource_keys={"email_notifier"},
)
def aggregate_global_revenue(
    context: OpExecutionContext,
    us_east_data: List[Dict[str, Any]],
    us_west_data: List[Dict[str, Any]],
    eu_data: List[Dict[str, Any]],
    apac_data: List[Dict[str, Any]],
) -> str:
    """Collect all converted regional data, calculate revenues, and generate global report."""
    logger.info("Aggregating global revenue data")
    
    # Calculate regional revenues
    def calculate_revenue(data: List[Dict[str, Any]], region: str) -> Dict[str, Any]:
        total = sum(float(record.get('amount', 0)) for record in data)
        record_count = len(data)
        logger.info(f"{region} revenue: ${total:,.2f} ({record_count} records)")
        return {
            "region": region,
            "revenue_usd": total,
            "record_count": record_count,
            "data": data,
        }
    
    regions = {
        "US-East": us_east_data,
        "US-West": us_west_data,
        "EU": eu_data,
        "APAC": apac_data,
    }
    
    regional_revenues = {name: calculate_revenue(data, name) for name, data in regions.items()}
    
    # Calculate global total
    global_revenue = sum(r["revenue_usd"] for r in regional_revenues.values())
    total_records = sum(r["record_count"] for r in regional_revenues.values())
    
    logger.info(f"Global revenue: ${global_revenue:,.2f} ({total_records} records)")
    
    # Generate report
    report_data = {
        "generated_at": datetime.utcnow().isoformat(),
        "global_revenue_usd": global_revenue,
        "total_records": total_records,
        "regional_breakdown": {
            region: {
                "revenue_usd": info["revenue_usd"],
                "record_count": info["record_count"],
            }
            for region, info in regional_revenues.items()
        },
    }
    
    # Write CSV report
    output_path = "/data/reports/global_revenue_report.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Region", "Revenue_USD", "Record_Count"])
        for region, info in regional_revenues.items():
            writer.writerow([region, f"{info['revenue_usd']:.2f}", info["record_count"]])
        writer.writerow([])
        writer.writerow(["Global Total", f"{global_revenue:.2f}", total_records])
    
    logger.info(f"Global revenue report saved to {output_path}")
    
    # Send notification (stub)
    try:
        email_config = context.resources.email_notifier
        logger.info(f"Email notification would be sent via {email_config['smtp_host']}")
    except Exception as e:
        logger.warning(f"Email notification failed: {e}")
    
    return output_path

@op(
    ins={"report_path": In(str, description="Path to global revenue report")},
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def end_pipeline(context: OpExecutionContext, report_path: str):
    """Marks pipeline completion."""
    logger.info(f"Pipeline completed successfully. Report: {report_path}")
    return None

# Job definition
@job(
    description="Multi-region ecommerce analytics pipeline with fan-out/fan-in pattern",
    resource_defs=resources,
)
def ecommerce_analytics_pipeline():
    # Start pipeline
    start = start_pipeline()
    
    # Fan-out phase 1: Ingestion
    us_east = ingest_us_east()
    us_west = ingest_us_west()
    eu = ingest_eu()
    apac = ingest_apac()
    
    # Fan-out phase 2: Conversion
    us_east_conv = convert_us_east(us_east)
    us_west_conv = convert_us_west(us_west)
    eu_conv = convert_eu(eu)
    apac_conv = convert_apac(apac)
    
    # Fan-in phase: Aggregation
    report = aggregate_global_revenue(us_east_conv, us_west_conv, eu_conv, apac_conv)
    
    # End pipeline
    end_pipeline(report)

# Schedule definition
daily_schedule = ScheduleDefinition(
    job=ecommerce_analytics_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    execution_timezone="UTC",
    description="Daily multi-region ecommerce analytics",
)

# Definitions object for Dagster deployment
defs = Definitions(
    jobs=[ecommerce_analytics_pipeline],
    schedules=[daily_schedule],
)

# Launch pattern
if __name__ == "__main__":
    # Execute the job in-process for testing
    result = ecommerce_analytics_pipeline.execute_in_process(
        run_config={
            "resources": {
                "csv_data": {"config": {"base_path": "/data/sales"}},
                "exchange_rates": {"config": {"rates": {"EUR_USD": 1.1, "JPY_USD": 0.007}}},
                "email_notifier": {"config": {"smtp_host": "smtp.example.com", "from_email": "dagster@example.com"}},
            }
        }
    )
    if result.success:
        print("Pipeline execution successful!")
    else:
        print("Pipeline execution failed!")