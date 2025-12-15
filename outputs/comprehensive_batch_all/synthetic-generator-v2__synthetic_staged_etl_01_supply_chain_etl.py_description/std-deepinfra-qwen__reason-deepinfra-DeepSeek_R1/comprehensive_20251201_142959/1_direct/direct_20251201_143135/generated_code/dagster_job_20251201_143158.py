from dagster import op, job, Out, In, ResourceDefinition, String, Field, execute_in_process

# Resource stubs
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
vendor_a_resource = ResourceDefinition.hardcoded_resource(VendorCSVResource("Vendor A"), "vendor_a_resource")
vendor_b_resource = ResourceDefinition.hardcoded_resource(VendorCSVResource("Vendor B"), "vendor_b_resource")
vendor_c_resource = ResourceDefinition.hardcoded_resource(VendorCSVResource("Vendor C"), "vendor_c_resource")
db_resource = ResourceDefinition.hardcoded_resource(DatabaseResource(), "db_resource")
email_resource = ResourceDefinition.hardcoded_resource(EmailResource(), "email_resource")

# Ops
@op(required_resource_keys={"vendor_a_resource"})
def ingest_vendor_a(context):
    """Ingests raw shipment CSV data from Vendor A."""
    data = context.resources.vendor_a_resource.read_csv()
    return data

@op(required_resource_keys={"vendor_b_resource"})
def ingest_vendor_b(context):
    """Ingests raw shipment CSV data from Vendor B."""
    data = context.resources.vendor_b_resource.read_csv()
    return data

@op(required_resource_keys={"vendor_c_resource"})
def ingest_vendor_c(context):
    """Ingests raw shipment CSV data from Vendor C."""
    data = context.resources.vendor_c_resource.read_csv()
    return data

@op(ins={"data_a": In(), "data_b": In(), "data_c": In()}, out=Out())
def cleanse_data(context, data_a, data_b, data_c):
    """Processes and normalizes data from all three vendors."""
    combined_data = [data_a, data_b, data_c]
    # Simulate data cleansing and normalization
    cleansed_data = [f"Cleansed {data}" for data in combined_data]
    return cleansed_data

@op(required_resource_keys={"db_resource"})
def load_to_db(context, cleansed_data):
    """Loads cleansed data to the PostgreSQL inventory database."""
    context.resources.db_resource.upsert_records(cleansed_data)

@op(required_resource_keys={"email_resource"})
def send_summary_email(context):
    """Sends a summary email notification to the supply chain team."""
    subject = "Supply Chain Shipment Data Processing Summary"
    body = "Processing date: 2024-01-01\nRecord counts: 3,850\nData quality metrics: Good"
    context.resources.email_resource.send_email(subject, body)

# Job
@job(
    resource_defs={
        "vendor_a_resource": vendor_a_resource,
        "vendor_b_resource": vendor_b_resource,
        "vendor_c_resource": vendor_c_resource,
        "db_resource": db_resource,
        "email_resource": email_resource,
    },
    config={
        "ops": {
            "ingest_vendor_a": {"config": {"vendor_name": "Vendor A"}},
            "ingest_vendor_b": {"config": {"vendor_name": "Vendor B"}},
            "ingest_vendor_c": {"config": {"vendor_name": "Vendor C"}},
        }
    },
    tags={"pipeline": "supply_chain_etl"},
)
def supply_chain_etl():
    data_a = ingest_vendor_a()
    data_b = ingest_vendor_b()
    data_c = ingest_vendor_c()
    cleansed_data = cleanse_data(data_a, data_b, data_c)
    loaded_data = load_to_db(cleansed_data)
    send_summary_email(loaded_data)

if __name__ == "__main__":
    result = supply_chain_etl.execute_in_process()