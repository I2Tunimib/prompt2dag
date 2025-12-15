from dagster import op, job, resource, RetryPolicy, Failure, Field, String, In, Out

# Resources
@resource(config_schema={"slack_webhook_url": Field(String)})
def slack_resource(context):
    return context.resource_config["slack_webhook_url"]

@resource(config_schema={"s3_bucket": Field(String)})
def s3_resource(context):
    return context.resource_config["s3_bucket"]

@resource(config_schema={"es_host": Field(String), "es_port": Field(String)})
def elasticsearch_resource(context):
    return {"host": context.resource_config["es_host"], "port": context.resource_config["es_port"]}

@resource(config_schema={"spark_jar": Field(String), "obo_parser_spark_jar": Field(String)})
def spark_resource(context):
    return {
        "spark_jar": context.resource_config["spark_jar"],
        "obo_parser_spark_jar": context.resource_config["obo_parser_spark_jar"]
    }

# Ops
@op(required_resource_keys={"slack_resource"})
def params_validate(context, color: str):
    """Validates the color parameter provided to the pipeline."""
    if color not in ["red", "green", "blue"]:
        raise ValueError("Invalid color parameter")
    context.log.info(f"Color parameter validated: {color}")

@op(required_resource_keys={"s3_resource"})
def download_mondo_terms(context):
    """Downloads the latest Mondo OBO file from GitHub releases and uploads it to S3 if new."""
    # Simplified logic for downloading and checking version
    latest_version = "1.0"  # Example version
    current_version = "0.9"  # Example current version
    if latest_version > current_version:
        context.log.info(f"Downloading new Mondo OBO file: {latest_version}")
        # Download and upload to S3
        context.log.info(f"Uploaded new Mondo OBO file to S3")
    else:
        context.log.info("No new Mondo OBO file available")

@op(required_resource_keys={"spark_resource"})
def normalized_mondo_terms(context):
    """Processes the Mondo OBO file using Spark to normalize and transform the ontology terms."""
    # Simplified Spark processing logic
    context.log.info("Processing Mondo OBO file using Spark")
    # Store results in the data lake
    context.log.info("Normalized Mondo terms stored in the data lake")

@op(required_resource_keys={"elasticsearch_resource", "spark_resource"})
def index_mondo_terms(context):
    """Indexes the normalized Mondo terms to Elasticsearch using a Spark job."""
    # Simplified Elasticsearch indexing logic
    context.log.info("Indexing normalized Mondo terms to Elasticsearch")
    # Index configuration
    context.log.info("Mondo terms indexed to Elasticsearch")

@op(required_resource_keys={"s3_resource"})
def publish_mondo(context):
    """Publishes the indexed Mondo data to make it available for consumption."""
    context.log.info("Publishing indexed Mondo data")
    # Publish logic
    context.log.info("Indexed Mondo data published")

@op(required_resource_keys={"slack_resource"})
def slack(context):
    """Sends a Slack notification indicating pipeline completion."""
    webhook_url = context.resources.slack_resource
    message = "Mondo ETL pipeline completed successfully"
    context.log.info(f"Sending Slack notification: {message}")
    # Send Slack notification
    context.log.info("Slack notification sent")

# Job
@job(
    resource_defs={
        "slack_resource": slack_resource,
        "s3_resource": s3_resource,
        "elasticsearch_resource": elasticsearch_resource,
        "spark_resource": spark_resource
    },
    retry_policy=RetryPolicy(max_retries=3),
    op_retry_policy=RetryPolicy(max_retries=3),
    tags={"pipeline": "mondo_etl"}
)
def mondo_etl_pipeline():
    color = "red"  # Example color parameter
    validated_color = params_validate(color)
    downloaded_terms = download_mondo_terms()
    normalized_terms = normalized_mondo_terms(start_after=[downloaded_terms])
    indexed_terms = index_mondo_terms(start_after=[normalized_terms])
    published_data = publish_mondo(start_after=[indexed_terms])
    slack(start_after=[published_data])

if __name__ == '__main__':
    result = mondo_etl_pipeline.execute_in_process(
        run_config={
            "resources": {
                "slack_resource": {"config": {"slack_webhook_url": "https://slack-webhook-url"}},
                "s3_resource": {"config": {"s3_bucket": "mondo-etl-bucket"}},
                "elasticsearch_resource": {"config": {"es_host": "localhost", "es_port": "9200"}},
                "spark_resource": {"config": {"spark_jar": "path/to/spark.jar", "obo_parser_spark_jar": "path/to/obo_parser_spark.jar"}}
            }
        }
    )