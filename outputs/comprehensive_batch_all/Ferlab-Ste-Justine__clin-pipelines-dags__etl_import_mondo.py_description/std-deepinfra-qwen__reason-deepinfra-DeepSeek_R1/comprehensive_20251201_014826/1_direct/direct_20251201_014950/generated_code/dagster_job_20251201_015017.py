from dagster import op, job, ResourceDefinition, String, Field, Failure, RetryRequested

# Simplified resource stubs
class S3Resource:
    def __init__(self):
        pass

    def upload_file(self, file_path, bucket, key):
        print(f"Uploading {file_path} to S3 bucket {bucket} with key {key}")

class SparkResource:
    def __init__(self):
        pass

    def process_data(self, input_path, output_path):
        print(f"Processing data from {input_path} to {output_path}")

class ElasticsearchResource:
    def __init__(self):
        pass

    def index_data(self, data, index_name):
        print(f"Indexing data to Elasticsearch index {index_name}")

class SlackResource:
    def __init__(self):
        pass

    def send_message(self, message):
        print(f"Sending Slack message: {message}")

# Resource definitions
s3_resource = ResourceDefinition.hardcoded_resource(S3Resource())
spark_resource = ResourceDefinition.hardcoded_resource(SparkResource())
elasticsearch_resource = ResourceDefinition.hardcoded_resource(ElasticsearchResource())
slack_resource = ResourceDefinition.hardcoded_resource(SlackResource())

# Op definitions
@op(config_schema={"color": Field(String)})
def params_validate(context):
    color = context.op_config["color"]
    if color not in ["red", "blue", "green"]:
        raise Failure(f"Invalid color: {color}")
    context.log.info(f"Validated color: {color}")

@op(required_resource_keys={"s3"})
def download_mondo_terms(context):
    # Download the latest Mondo OBO file from GitHub releases
    # Check if a newer version exists compared to what's already imported
    # Upload the new file to S3 if available
    context.log.info("Downloading and uploading Mondo OBO file to S3")
    context.resources.s3.upload_file("path/to/mondo.obo", "mondo-bucket", "mondo.obo")

@op(required_resource_keys={"spark"})
def normalized_mondo_terms(context):
    # Process the Mondo OBO file using Spark to normalize and transform the ontology terms
    # Store results in the data lake
    context.log.info("Normalizing and transforming Mondo terms using Spark")
    context.resources.spark.process_data("s3://mondo-bucket/mondo.obo", "s3://mondo-bucket/normalized-mondo")

@op(required_resource_keys={"elasticsearch"})
def index_mondo_terms(context):
    # Index the normalized Mondo terms to Elasticsearch using a Spark job with specific index configuration
    context.log.info("Indexing normalized Mondo terms to Elasticsearch")
    context.resources.elasticsearch.index_data("s3://mondo-bucket/normalized-mondo", "mondo_index")

@op
def publish_mondo(context):
    # Publish the indexed Mondo data to make it available for consumption
    context.log.info("Publishing indexed Mondo data")

@op(required_resource_keys={"slack"})
def slack(context):
    # Send a Slack notification indicating pipeline completion
    context.log.info("Sending Slack notification")
    context.resources.slack.send_message("Mondo ETL pipeline completed successfully")

# Job definition
@job(
    resource_defs={
        "s3": s3_resource,
        "spark": spark_resource,
        "elasticsearch": elasticsearch_resource,
        "slack": slack_resource,
    }
)
def mondo_etl_pipeline():
    params_validate_op = params_validate()
    download_mondo_terms_op = download_mondo_terms(params_validate_op)
    normalized_mondo_terms_op = normalized_mondo_terms(download_mondo_terms_op)
    index_mondo_terms_op = index_mondo_terms(normalized_mondo_terms_op)
    publish_mondo_op = publish_mondo(index_mondo_terms_op)
    slack(publish_mondo_op)

# Launch pattern
if __name__ == "__main__":
    result = mondo_etl_pipeline.execute_in_process(
        run_config={
            "ops": {
                "params_validate": {
                    "config": {
                        "color": "red"
                    }
                }
            }
        }
    )