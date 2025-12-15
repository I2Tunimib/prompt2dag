from dagster import op, job, resource, RetryPolicy, Failure, Field, String, In, Out

@resource(config_schema={"exchange_rates": Field(dict)})
def exchange_rate_resource(context):
    return context.resource_config["exchange_rates"]

@op
def start_pipeline():
    """Marks the start of the pipeline."""
    pass

@op
def ingest_us_east_sales_data():
    """Ingests US-East region sales data from CSV source."""
    # Simulate data ingestion
    return [{"region": "US-East", "sales": 10000}]

@op
def ingest_us_west_sales_data():
    """Ingests US-West region sales data from CSV source."""
    # Simulate data ingestion
    return [{"region": "US-West", "sales": 15000}]

@op
def ingest_eu_sales_data():
    """Ingests EU region sales data from CSV source."""
    # Simulate data ingestion
    return [{"region": "EU", "sales": 12000}]

@op
def ingest_apac_sales_data():
    """Ingests APAC region sales data from CSV source."""
    # Simulate data ingestion
    return [{"region": "APAC", "sales": 18000}]

@op(ins={"data": In(dagster_type=list)}, config_schema={"region": String})
def convert_currency_to_usd(context, data, exchange_rate_resource):
    """Converts sales data to USD using exchange rates."""
    region = context.op_config["region"]
    if region in ["US-East", "US-West"]:
        return data
    else:
        exchange_rate = exchange_rate_resource[region]
        return [{"region": entry["region"], "sales": entry["sales"] * exchange_rate} for entry in data]

@op(ins={"us_east_data": In(dagster_type=list),
         "us_west_data": In(dagster_type=list),
         "eu_data": In(dagster_type=list),
         "apac_data": In(dagster_type=list)})
def aggregate_sales_data(us_east_data, us_west_data, eu_data, apac_data):
    """Aggregates all converted regional data and generates a global revenue report."""
    all_data = us_east_data + us_west_data + eu_data + apac_data
    total_revenue = sum(entry["sales"] for entry in all_data)
    report = {"total_revenue": total_revenue, "data": all_data}
    return report

@op
def end_pipeline():
    """Marks the end of the pipeline."""
    pass

@job(
    resource_defs={"exchange_rate_resource": exchange_rate_resource},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Multi-region ecommerce analytics pipeline",
)
def ecommerce_analytics_pipeline():
    start = start_pipeline()
    
    us_east_data = ingest_us_east_sales_data(start)
    us_west_data = ingest_us_west_sales_data(start)
    eu_data = ingest_eu_sales_data(start)
    apac_data = ingest_apac_sales_data(start)
    
    us_east_converted = convert_currency_to_usd.alias("convert_us_east_to_usd")(us_east_data, region="US-East")
    us_west_converted = convert_currency_to_usd.alias("convert_us_west_to_usd")(us_west_data, region="US-West")
    eu_converted = convert_currency_to_usd.alias("convert_eu_to_usd")(eu_data, region="EU")
    apac_converted = convert_currency_to_usd.alias("convert_apac_to_usd")(apac_data, region="APAC")
    
    aggregated_data = aggregate_sales_data(us_east_converted, us_west_converted, eu_converted, apac_converted)
    
    end_pipeline(aggregated_data)

if __name__ == "__main__":
    result = ecommerce_analytics_pipeline.execute_in_process(
        resources={"exchange_rate_resource": {"config": {"exchange_rates": {"EU": 1.1, "APAC": 0.009}}}}
    )