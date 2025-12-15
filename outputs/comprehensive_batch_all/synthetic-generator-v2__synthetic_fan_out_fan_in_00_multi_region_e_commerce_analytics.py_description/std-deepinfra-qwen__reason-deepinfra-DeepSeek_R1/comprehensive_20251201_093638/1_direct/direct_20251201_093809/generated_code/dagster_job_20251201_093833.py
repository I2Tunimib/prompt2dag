from dagster import op, job, resource, RetryPolicy, Failure, Field, String, In, Out, Output

@resource(config_schema={"exchange_rates": Field(dict)})
def exchange_rate_resource(context):
    return context.resource_config["exchange_rates"]

@op
def start_pipeline():
    """Marks the start of the pipeline."""
    pass

@op
def ingest_sales_data(region: str) -> list:
    """Ingests sales data from a CSV file for the specified region."""
    # Simulate data ingestion
    if region == "US-East":
        return [{"region": "US-East", "sales": 1000}]
    elif region == "US-West":
        return [{"region": "US-West", "sales": 1500}]
    elif region == "EU":
        return [{"region": "EU", "sales": 1200, "currency": "EUR"}]
    elif region == "APAC":
        return [{"region": "APAC", "sales": 2000, "currency": "JPY"}]
    else:
        raise ValueError(f"Unknown region: {region}")

@op(ins={"data": In(list)}, out=Out(list), required_resource_keys={"exchange_rates"})
def convert_currency(data, context):
    """Converts sales data to USD using exchange rates."""
    exchange_rates = context.resources.exchange_rates
    converted_data = []
    for record in data:
        if record["currency"] == "EUR":
            record["sales"] *= exchange_rates["EUR"]
        elif record["currency"] == "JPY":
            record["sales"] *= exchange_rates["JPY"]
        del record["currency"]
        converted_data.append(record)
    return converted_data

@op(ins={"us_east_data": In(list), "us_west_data": In(list), "eu_data": In(list), "apac_data": In(list)})
def aggregate_sales_data(us_east_data, us_west_data, eu_data, apac_data):
    """Aggregates sales data from all regions and generates a global revenue report."""
    all_data = us_east_data + us_west_data + eu_data + apac_data
    total_revenue = sum(record["sales"] for record in all_data)
    report = f"Global Revenue Report: Total Revenue = {total_revenue} USD"
    return report

@op
def end_pipeline():
    """Marks the end of the pipeline."""
    pass

@job(
    resource_defs={"exchange_rates": exchange_rate_resource},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ecommerce_analytics_pipeline():
    start = start_pipeline()
    
    us_east_data = ingest_sales_data.alias("ingest_us_east_data")(region="US-East")
    us_west_data = ingest_sales_data.alias("ingest_us_west_data")(region="US-West")
    eu_data = ingest_sales_data.alias("ingest_eu_data")(region="EU")
    apac_data = ingest_sales_data.alias("ingest_apac_data")(region="APAC")
    
    us_east_converted = convert_currency.alias("convert_us_east_currency")(data=us_east_data)
    us_west_converted = convert_currency.alias("convert_us_west_currency")(data=us_west_data)
    eu_converted = convert_currency.alias("convert_eu_currency")(data=eu_data)
    apac_converted = convert_currency.alias("convert_apac_currency")(data=apac_data)
    
    aggregated_report = aggregate_sales_data(
        us_east_data=us_east_converted,
        us_west_data=us_west_converted,
        eu_data=eu_converted,
        apac_data=apac_converted,
    )
    
    end = end_pipeline()
    
    start >> [us_east_data, us_west_data, eu_data, apac_data] >> [us_east_converted, us_west_converted, eu_converted, apac_converted] >> aggregated_report >> end

if __name__ == "__main__":
    result = ecommerce_analytics_pipeline.execute_in_process(
        resources={"exchange_rates": {"config": {"exchange_rates": {"EUR": 1.1, "JPY": 0.007}}}}
    )
    print(result.output_for_node("aggregate_sales_data"))