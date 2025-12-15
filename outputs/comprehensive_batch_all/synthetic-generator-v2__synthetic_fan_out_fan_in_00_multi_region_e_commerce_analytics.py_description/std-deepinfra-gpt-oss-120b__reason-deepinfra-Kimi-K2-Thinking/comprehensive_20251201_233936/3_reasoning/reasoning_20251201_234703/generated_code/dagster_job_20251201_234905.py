from dagster import (
    op,
    job,
    In,
    Out,
    Nothing,
    RetryPolicy,
    ScheduleDefinition,
    Definitions,
    resource,
    Config,
    OpExecutionContext,
)
from typing import List, Dict, Any
import csv
import io
from datetime import datetime


# -----------------
# Resources
# -----------------

@resource
def csv_data_source():
    """Resource for accessing CSV data files."""
    return {
        "us_east": "data/us_east_sales.csv",
        "us_west": "data/us_west_sales.csv",
        "eu": "data/eu_sales.csv",
        "apac": "data/apac_sales.csv",
    }


@resource
def exchange_rate_client():
    """Resource for fetching exchange rates."""
    # In production, this would call an API
    # For demo, return static rates
    return {
        "EUR_TO_USD": 1.08,
        "JPY_TO_USD": 0.0067,
    }


@resource
def report_writer():
    """Resource for writing final reports."""
    return {
        "output_path": "reports/global_revenue_report.csv",
    }


# -----------------
# Ops
# -----------------

@op(
    out=Out(Nothing),
    description="Marks the start of the pipeline",
)
def start_pipeline(context: OpExecutionContext):
    """Initialize pipeline execution."""
    context.log.info("Starting multi-region ecommerce analytics pipeline")
    return Nothing


@op(
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest US-East region sales data from CSV",
)
def ingest_us_east(context: OpExecutionContext, csv_source) -> List[Dict[str, Any]]:
    """Ingest sales data for US-East region."""
    context.log.info("Ingesting US-East region data")
    
    # Simulate CSV reading
    # In real implementation: read from csv_source["us_east"]
    data = [
        {"region": "US-East", "sales": 10000, "currency": "USD", "date": "2024-01-01"},
        {"region": "US-East", "sales": 15000, "currency": "USD", "date": "2024-01-02"},
    ]
    return data


@op(
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest US-West region sales data from CSV",
)
def ingest_us_west(context: OpExecutionContext, csv_source) -> List[Dict[str, Any]]:
    """Ingest sales data for US-West region."""
    context.log.info("Ingesting US-West region data")
    
    data = [
        {"region": "US-West", "sales": 12000, "currency": "USD", "date": "2024-01-01"},
        {"region": "US-West", "sales": 18000, "currency": "USD", "date": "2024-01-02"},
    ]
    return data


@op(
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest EU region sales data from CSV",
)
def ingest_eu(context: OpExecutionContext, csv_source) -> List[Dict[str, Any]]:
    """Ingest sales data for EU region."""
    context.log.info("Ingesting EU region data")
    
    data = [
        {"region": "EU", "sales": 9000, "currency": "EUR", "date": "2024-01-01"},
        {"region": "EU", "sales": 11000, "currency": "EUR", "date": "2024-01-02"},
    ]
    return data


@op(
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest APAC region sales data from CSV",
)
def ingest_apac(context: OpExecutionContext, csv_source) -> List[Dict[str, Any]]:
    """Ingest sales data for APAC region."""
    context.log.info("Ingesting APAC region data")
    
    data = [
        {"region": "APAC", "sales": 1000000, "currency": "JPY", "date": "2024-01-01"},
        {"region": "APAC", "sales": 1500000, "currency": "JPY", "date": "2024-01-02"},
    ]
    return data


@op(
    ins={"raw_data": In(List[Dict[str, Any]])},
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Convert US-East data (already USD)",
)
def convert_us_east(context: OpExecutionContext, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert US-East data to USD (no conversion needed)."""
    context.log.info("Converting US-East data (USD passthrough)")
    
    # Already in USD, just add converted_amount
    for row in raw_data:
        row["converted_amount"] = row["sales"]
        row["converted_currency"] = "USD"
    
    return raw_data


@op(
    ins={"raw_data": In(List[Dict[str, Any]])},
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Convert US-West data (already USD)",
)
def convert_us_west(context: OpExecutionContext, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert US-West data to USD (no conversion needed)."""
    context.log.info("Converting US-West data (USD passthrough)")
    
    for row in raw_data:
        row["converted_amount"] = row["sales"]
        row["converted_currency"] = "USD"
    
    return raw_data


@op(
    ins={"raw_data": In(List[Dict[str, Any]])},
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Convert EU data from EUR to USD",
)
def convert_eu(context: OpExecutionContext, raw_data: List[Dict[str, Any]], exchange_rate_client) -> List[Dict[str, Any]]:
    """Convert EU data from EUR to USD using exchange rates."""
    context.log.info("Converting EU data from EUR to USD")
    
    rate = exchange_rate_client["EUR_TO_USD"]
    
    for row in raw_data:
        row["converted_amount"] = row["sales"] * rate
        row["converted_currency"] = "USD"
        row["exchange_rate"] = rate
    
    return raw_data


@op(
    ins={"raw_data": In(List[Dict[str, Any]])},
    out=Out(List[Dict[str, Any]]),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Convert APAC data from JPY to USD",
)
def convert_apac(context: OpExecutionContext, raw_data: List[Dict[str, Any]], exchange_rate_client) -> List[Dict[str, Any]]:
    """Convert APAC data from JPY to USD using exchange rates."""
    context.log.info("Converting APAC data from JPY to USD")
    
    rate = exchange_rate_client["JPY_TO_USD"]
    
    for row in raw_data:
        row["converted_amount"] = row["sales"] * rate
        row["converted_currency"] = "USD"
        row["exchange_rate"] = rate
    
    return raw_data


@op(
    ins={
        "us_east_data": In(List[Dict[str, Any]]),
        "us_west_data": In(List[Dict[str, Any]]),
        "eu_data": In(List[Dict[str, Any]]),
        "apac_data": In(List[Dict[str, Any]]),
    },
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Aggregate all regional data and generate global revenue report",
)
def aggregate_global_revenue(
    context: OpExecutionContext,
    us_east_data: List[Dict[str, Any]],
    us_west_data: List[Dict[str, Any]],
    eu_data: List[Dict[str, Any]],
    apac_data: List[Dict[str, Any]],
    report_writer,
) -> Nothing:
    """Collect all converted regional data, calculate revenues, and generate final CSV report."""
    context.log.info("Aggregating all regional data for global revenue report")
    
    # Combine all data
    all_data = us_east_data + us_west_data + eu_data + apac_data
    
    # Calculate regional revenues
    regional_revenues = {}
    for row in all_data:
        region = row["region"]
        amount = row["converted_amount"]
        regional_revenues[region] = regional_revenues.get(region, 0) + amount
    
    # Calculate global total
    global_total = sum(regional_revenues.values())
    
    # Generate report
    output_path = report_writer["output_path"]
    
    # Write CSV report
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Region", "Revenue_USD", "Report_Date"])
        
        for region, revenue in regional_revenues.items():
            writer.writerow([region, f"{revenue:.2f}", datetime.now().isoformat()])
        
        writer.writerow([])
        writer.writerow(["Global_Total", f"{global_total:.2f}", datetime.now().isoformat()])
    
    context.log.info(f"Global revenue report generated: {output_path}")
    context.log.info(f"Regional revenues: {regional_revenues}")
    context.log.info(f"Global total: {global_total:.2f} USD")
    
    return Nothing


@op(
    ins={"_upstream": In(Nothing)},
    out=Out(Nothing),
    description="Marks the end of the pipeline",
)
def end_pipeline(context: OpExecutionContext, _upstream: Nothing) -> Nothing:
    """Finalize pipeline execution."""
    context.log.info("Multi-region ecommerce analytics pipeline completed successfully")
    return Nothing


# -----------------
# Job Definition
# -----------------

@job(
    description="Multi-region ecommerce analytics pipeline with fan-out/fan-in pattern",
    resource_defs={
        "csv_source": csv_data_source,
        "exchange_rate_client": exchange_rate_client,
        "report_writer": report_writer,
    },
)
def multi_region_ecommerce_pipeline():
    """Define the pipeline with all dependencies."""
    
    # Start pipeline
    start = start_pipeline()
    
    # Fan-out phase 1: Ingestion
    us_east_raw = ingest_us_east(start)
    us_west_raw = ingest_us_west(start)
    eu_raw = ingest_eu(start)
    apac_raw = ingest_apac(start)
    
    # Fan-out phase 2: Conversion
    us_east_converted = convert_us_east(us_east_raw)
    us_west_converted = convert_us_west(us_west_raw)
    eu_converted = convert_eu(eu_raw)
    apac_converted = convert_apac(apac_raw)
    
    # Fan-in phase: Aggregation
    aggregated = aggregate_global_revenue(
        us_east_data=us_east_converted,
        us_west_data=us_west_converted,
        eu_data=eu_converted,
        apac_data=apac_converted,
    )
    
    # End pipeline
    end_pipeline(aggregated)


# -----------------
# Schedule
# -----------------

daily_schedule = ScheduleDefinition(
    job=multi_region_ecommerce_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    execution_timezone="UTC",
    description="Daily execution of multi-region ecommerce analytics pipeline",
)


# -----------------
# Definitions
# -----------------

defs = Definitions(
    jobs=[multi_region_ecommerce_pipeline],
    schedules=[daily_schedule],
)


# -----------------
# Launch Pattern
# -----------------

if __name__ == "__main__":
    # Execute the job in-process for testing
    result = multi_region_ecommerce_pipeline.execute_in_process()
    
    if result.success:
        print("Pipeline execution successful!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")