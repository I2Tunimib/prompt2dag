from dagster import op, job, RetryPolicy, In, Out, Output, graph

# Resources and Configs
# Example resource for email system
# class EmailResource:
#     def send_email(self, recipient, subject, body):
#         pass

# Example resource for database
# class DatabaseResource:
#     def load_data(self, data):
#         pass

# Example resource for quarantine storage
# class QuarantineResource:
#     def store_data(self, data):
#         pass

# Example resource for file system
# class FileSystemResource:
#     def read_file(self, path):
#         pass

# Ops
@op(
    description="Ingests raw customer CSV data from source location and returns file metadata",
    out=Out(dict, description="File metadata"),
)
def ingest_csv(context):
    # Example file system resource usage
    # file_system = context.resources.file_system
    # file_metadata = file_system.read_file("path/to/customer.csv")
    file_metadata = {"path": "path/to/customer.csv", "size": 1024, "rows": 1000}
    return file_metadata

@op(
    description="Calculates data quality score using completeness and validity metrics",
    ins={"file_metadata": In(dict, description="File metadata")},
    out=Out(float, description="Quality score"),
)
def quality_check(context, file_metadata):
    # Example quality check logic
    quality_score = 0.96  # Simulated quality score
    return quality_score

@op(
    description="Loads high-quality data to production database",
    ins={"file_metadata": In(dict, description="File metadata")},
)
def production_load(context, file_metadata):
    # Example database resource usage
    # database = context.resources.database
    # database.load_data(file_metadata)
    context.log.info("Data loaded to production database")

@op(
    description="Moves low-quality data to quarantine and sends an alert email",
    ins={"file_metadata": In(dict, description="File metadata")},
)
def quarantine_and_alert(context, file_metadata):
    # Example quarantine resource usage
    # quarantine = context.resources.quarantine
    # quarantine.store_data(file_metadata)
    context.log.info("Data moved to quarantine")

@op(
    description="Sends an alert email to data stewards",
    ins={"file_metadata": In(dict, description="File metadata")},
)
def send_alert_email(context, file_metadata):
    # Example email resource usage
    # email = context.resources.email
    # email.send_email("data-stewards@example.com", "Data Quality Alert", "Low quality data detected")
    context.log.info("Alert email sent to data stewards")

@op(
    description="Performs final cleanup operations for temporary files and resources",
    ins={"file_metadata": In(dict, description="File metadata")},
)
def cleanup(context, file_metadata):
    # Example cleanup logic
    context.log.info("Cleanup operations completed")

# Job
@job(
    description="Data quality gate pipeline for customer CSV data",
    resource_defs={
        # "file_system": FileSystemResource,
        # "database": DatabaseResource,
        # "quarantine": QuarantineResource,
        # "email": EmailResource,
    },
    retry_policy=RetryPolicy(max_retries=1, delay=300),
)
def data_quality_gate():
    file_metadata = ingest_csv()
    quality_score = quality_check(file_metadata)

    production_load_op = production_load(file_metadata).with_hooks({quality_score >= 0.95})
    quarantine_and_alert_op = quarantine_and_alert(file_metadata).with_hooks({quality_score < 0.95})
    send_alert_email_op = send_alert_email(quarantine_and_alert_op)

    cleanup_op = cleanup(file_metadata)
    production_load_op >> cleanup_op
    send_alert_email_op >> cleanup_op

if __name__ == '__main__':
    result = data_quality_gate.execute_in_process()