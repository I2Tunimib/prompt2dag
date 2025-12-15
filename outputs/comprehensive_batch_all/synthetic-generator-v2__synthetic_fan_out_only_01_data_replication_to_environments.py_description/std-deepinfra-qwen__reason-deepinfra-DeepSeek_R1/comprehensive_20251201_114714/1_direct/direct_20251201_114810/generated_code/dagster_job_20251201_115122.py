from dagster import op, job, RetryPolicy, ResourceDefinition, Field, String, In, Out

# Simplified resource stubs
class DatabaseResource:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def execute_query(self, query):
        # Simulate database query execution
        print(f"Executing query: {query} on {self.connection_string}")

class FileSystemResource:
    def __init__(self, temp_dir):
        self.temp_dir = temp_dir

    def write_csv(self, data, filename):
        # Simulate writing CSV to file system
        print(f"Writing CSV to {self.temp_dir}/{filename}")

    def read_csv(self, filename):
        # Simulate reading CSV from file system
        print(f"Reading CSV from {self.temp_dir}/{filename}")
        return f"Data from {filename}"

# Resources
prod_db_resource = ResourceDefinition.hardcoded_resource(
    DatabaseResource("prod_db_connection_string"), "prod_db"
)
dev_db_resource = ResourceDefinition.hardcoded_resource(
    DatabaseResource("dev_db_connection_string"), "dev_db"
)
staging_db_resource = ResourceDefinition.hardcoded_resource(
    DatabaseResource("staging_db_connection_string"), "staging_db"
)
qa_db_resource = ResourceDefinition.hardcoded_resource(
    DatabaseResource("qa_db_connection_string"), "qa_db"
)
file_system_resource = ResourceDefinition.hardcoded_resource(
    FileSystemResource("/tmp/csv_snapshots"), "file_system"
)

# Ops
@op(required_resource_keys={"prod_db", "file_system"}, tags={"type": "database", "action": "dump"})
def dump_prod_csv(context):
    """Dump production database to a CSV file."""
    date_str = context.logical_date.strftime("%Y-%m-%d")
    filename = f"prod_snapshot_{date_str}.csv"
    data = context.resources.prod_db.execute_query("SELECT * FROM production_table")
    context.resources.file_system.write_csv(data, filename)
    return filename

@op(required_resource_keys={"file_system", "dev_db"}, tags={"type": "database", "action": "copy"})
def copy_dev(context, filename: str):
    """Copy CSV snapshot to the Development database."""
    data = context.resources.file_system.read_csv(filename)
    context.resources.dev_db.execute_query(f"LOAD DATA INFILE '{filename}' INTO TABLE dev_table")

@op(required_resource_keys={"file_system", "staging_db"}, tags={"type": "database", "action": "copy"})
def copy_staging(context, filename: str):
    """Copy CSV snapshot to the Staging database."""
    data = context.resources.file_system.read_csv(filename)
    context.resources.staging_db.execute_query(f"LOAD DATA INFILE '{filename}' INTO TABLE staging_table")

@op(required_resource_keys={"file_system", "qa_db"}, tags={"type": "database", "action": "copy"})
def copy_qa(context, filename: str):
    """Copy CSV snapshot to the QA database."""
    data = context.resources.file_system.read_csv(filename)
    context.resources.qa_db.execute_query(f"LOAD DATA INFILE '{filename}' INTO TABLE qa_table")

# Job
@job(
    resource_defs={
        "prod_db": prod_db_resource,
        "dev_db": dev_db_resource,
        "staging_db": staging_db_resource,
        "qa_db": qa_db_resource,
        "file_system": file_system_resource,
    },
    tags={"owner": "data_engineering", "category": "replication", "type": "database", "pattern": "fanout"},
    description="Daily database replication pipeline",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def daily_database_replication():
    filename = dump_prod_csv()
    copy_dev(filename)
    copy_staging(filename)
    copy_qa(filename)

# Launch pattern
if __name__ == "__main__":
    result = daily_database_replication.execute_in_process()