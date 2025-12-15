from dagster import op, job, In, Out, ResourceDefinition, String, RetryPolicy, execute_in_process

# Simplified resource stubs
class VendorCSVResource:
    def __init__(self, vendor_name):
        self.vendor_name = vendor_name

    def read_csv(self):
        # Simulate reading CSV data
        return f"Data from {self.vendor_name}"

class DatabaseResource:
    def upsert_records(self, data):
        # Simulate upserting records to the database
        print(f"Upserted {len(data)} records to the database")

class EmailResource:
    def send_email(self, subject, body):
        # Simulate sending an email
        print(f"Sent email with subject: {subject} and body: {body}")

# Resources
vendor_a_csv = ResourceDefinition.hardcoded_resource(VendorCSVResource("Vendor A"), "vendor_a_csv")
vendor_b_csv = ResourceDefinition.hardcoded_resource(VendorCSVResource("Vendor B"), "vendor_b_csv")
vendor_c_csv = ResourceDefinition.hardcoded_resource(VendorCSVResource("Vendor C"), "vendor_c_csv")
database = ResourceDefinition.hardcoded_resource(DatabaseResource(), "database")
email = ResourceDefinition.hardcoded_resource(EmailResource(), "email")

# Ops
@op(required_resource_keys={"vendor_csv"}, out=Out(String))
def ingest_vendor(context):
    data = context.resources.vendor_csv.read_csv()
    return data

@op(ins={"data_a": In(String), "data_b": In(String), "data_c": In(String)}, out=Out(String))
def cleanse_data(data_a, data_b, data_c):
    # Simulate data cleansing and normalization
    combined_data = f"Cleaned data from {data_a}, {data_b}, and {data_c}"
    return combined_data

@op(required_resource_keys={"database"}, ins={"cleaned_data": In(String)})
def load_to_db(context, cleaned_data):
    # Simulate loading data to the database
    context.resources.database.upsert_records(cleaned_data.split(", "))

@op(required_resource_keys={"email"}, ins={"processing_date": In(String), "record_count": In(int), "data_quality_metrics": In(String)})
def send_summary_email(context, processing_date, record_count, data_quality_metrics):
    # Simulate sending a summary email
    subject = f"Supply Chain Shipment Data Processing Summary - {processing_date}"
    body = f"Processed {record_count} records. Data quality metrics: {data_quality_metrics}"
    context.resources.email.send_email(subject, body)

# Job
@job(
    resource_defs={
        "vendor_a_csv": vendor_a_csv,
        "vendor_b_csv": vendor_b_csv,
        "vendor_c_csv": vendor_c_csv,
        "database": database,
        "email": email
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def supply_chain_etl():
    vendor_a_data = ingest_vendor.alias("ingest_vendor_a")()
    vendor_b_data = ingest_vendor.alias("ingest_vendor_b")()
    vendor_c_data = ingest_vendor.alias("ingest_vendor_c")()

    cleaned_data = cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data)

    loaded_data = load_to_db(cleaned_data)

    send_summary_email(
        processing_date="2024-01-01",
        record_count=3850,
        data_quality_metrics="99.5% valid records",
    ).with_hooks({loaded_data})

if __name__ == "__main__":
    result = supply_chain_etl.execute_in_process()