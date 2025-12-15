from dagster import op, job, resource, RetryPolicy, In, Out, Output, graph

# Resources
@resource
def postgres_db_resource():
    # Simplified resource for PostgreSQL database connection
    pass

@resource
def email_resource():
    # Simplified resource for sending emails
    pass

# Ops
@op
def ingest_vendor_a():
    """Ingests raw shipment CSV data from Vendor A."""
    # Simulate data ingestion
    data = [{"sku": "A123", "date": "2024-01-01", "quantity": 100}]
    return data

@op
def ingest_vendor_b():
    """Ingests raw shipment CSV data from Vendor B."""
    # Simulate data ingestion
    data = [{"sku": "B456", "date": "2024-01-01", "quantity": 150}]
    return data

@op
def ingest_vendor_c():
    """Ingests raw shipment CSV data from Vendor C."""
    # Simulate data ingestion
    data = [{"sku": "C789", "date": "2024-01-01", "quantity": 200}]
    return data

@op(ins={"data_a": In(), "data_b": In(), "data_c": In()}, out=Out())
def cleanse_data(data_a, data_b, data_c):
    """Processes all three vendor datasets together for cleansing and normalization."""
    # Combine data
    combined_data = data_a + data_b + data_c
    
    # Simulate data cleansing and normalization
    cleansed_data = [
        {**record, "sku": record["sku"].upper(), "valid": True}
        for record in combined_data
    ]
    
    return cleansed_data

@op(required_resource_keys={"postgres_db"})
def load_to_db(context, cleansed_data):
    """Loads cleansed data to PostgreSQL inventory database."""
    # Simulate database loading
    context.log.info(f"Loading {len(cleansed_data)} records to database.")
    # Upsert records to inventory_shipments table
    for record in cleansed_data:
        context.resources.postgres_db.upsert(record)

@op(required_resource_keys={"email"})
def send_summary_email(context, cleansed_data):
    """Sends email notification to supply chain team."""
    # Simulate email sending
    record_count = len(cleansed_data)
    context.log.info(f"Sending summary email with {record_count} records.")
    context.resources.email.send(
        to="supply-chain-team@company.com",
        subject="Supply Chain Shipment Data Processing Summary",
        body=f"Processed {record_count} records on {context.run_start_time}."
    )

# Job
@graph
def supply_chain_pipeline():
    vendor_a_data = ingest_vendor_a()
    vendor_b_data = ingest_vendor_b()
    vendor_c_data = ingest_vendor_c()
    
    cleansed_data = cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data)
    
    loaded_data = load_to_db(cleansed_data)
    
    send_summary_email(loaded_data)

# Job configuration
supply_chain_job = supply_chain_pipeline.to_job(
    name="supply_chain_job",
    resource_defs={
        "postgres_db": postgres_db_resource,
        "email": email_resource,
    },
    tags={"dagster/priority": "1"},
    description="Three-stage ETL pipeline for supply chain shipment data processing.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)

# Launch pattern
if __name__ == '__main__':
    result = supply_chain_job.execute_in_process()