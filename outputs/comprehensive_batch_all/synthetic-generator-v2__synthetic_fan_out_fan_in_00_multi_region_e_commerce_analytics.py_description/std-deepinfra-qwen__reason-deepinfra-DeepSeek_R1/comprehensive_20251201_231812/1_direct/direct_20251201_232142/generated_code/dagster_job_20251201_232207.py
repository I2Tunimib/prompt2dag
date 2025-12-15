from dagster import op, job, resource, RetryPolicy, Failure, Field, String, In, Out

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
    return [{"region": region, "sales": 10000}]

@op(ins={"data": In(list)})
def convert_currency(data: list, region: str, exchange_rate_resource) -> list:
    """Converts sales data to USD for the specified region."""
    if region in ["US-East", "US-West"]:
        return data
    else:
        exchange_rate = exchange_rate_resource[region]
        return [{"region": region, "sales": item["sales"] * exchange_rate} for item in data]

@op(ins={"data": In(list)}, out=Out(list))
def aggregate_data(data: list):
    """Aggregates all regional data and generates a global revenue report."""
    total_revenue = sum(item["sales"] for item in data)
    report = {"total_revenue": total_revenue, "regions": data}
    return [report]

@op
def end_pipeline():
    """Marks the end of the pipeline."""
    pass

@job(
    resource_defs={"exchange_rate_resource": exchange_rate_resource},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ecommerce_analytics_pipeline():
    start = start_pipeline()

    us_east_data = ingest_sales_data.alias("ingest_us_east_sales_data")(region="US-East")
    us_west_data = ingest_sales_data.alias("ingest_us_west_sales_data")(region="US-West")
    eu_data = ingest_sales_data.alias("ingest_eu_sales_data")(region="EU")
    apac_data = ingest_sales_data.alias("ingest_apac_sales_data")(region="APAC")

    us_east_converted = convert_currency.alias("convert_us_east_currency")(
        data=us_east_data, region="US-East", exchange_rate_resource=exchange_rate_resource
    )
    us_west_converted = convert_currency.alias("convert_us_west_currency")(
        data=us_west_data, region="US-West", exchange_rate_resource=exchange_rate_resource
    )
    eu_converted = convert_currency.alias("convert_eu_currency")(
        data=eu_data, region="EU", exchange_rate_resource=exchange_rate_resource
    )
    apac_converted = convert_currency.alias("convert_apac_currency")(
        data=apac_data, region="APAC", exchange_rate_resource=exchange_rate_resource
    )

    aggregated_data = aggregate_data(
        data=[us_east_converted, us_west_converted, eu_converted, apac_converted]
    )

    end = end_pipeline()

    start >> [us_east_data, us_west_data, eu_data, apac_data]
    [us_east_converted, us_west_converted, eu_converted, apac_converted] >> aggregated_data >> end

if __name__ == "__main__":
    result = ecommerce_analytics_pipeline.execute_in_process(
        resources={"exchange_rate_resource": {"config": {"exchange_rates": {"EU": 1.1, "APAC": 0.009}}}}
    )