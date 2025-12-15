from dagster import op, job, In, Out, resource, HookContext, failure_hook
from typing import Dict, Any

# Minimal resource implementations (stubs for demonstration)
class S3Client:
    def upload_file(self, file_path: str, bucket: str, key: str) -> str:
        print(f"Uploading {file_path} to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"
    
    def file_exists(self, bucket: str, key: str) -> bool:
        # Simulate version check - real implementation would check S3
        return False

class SparkClient:
    def __init__(self, jar_path: str, obo_parser_jar: str):
        self.jar_path = jar_path
        self.obo_parser_jar = obo_parser_jar
    
    def run_normalization(self, input_path: str, output_path: str) -> str:
        print(f"Running Spark normalization with {self.jar_path}")
        print(f"OBO parser: {self.obo_parser_jar}")
        print(f"Input: {input_path}, Output: {output_path}")
        return output_path
    
    def run_indexing(self, input_path: str, es_host: str, es_index: str) -> str:
        print(f"Running Spark indexing with {self.jar_path}")
        print(f"Input: {input_path}, ES: {es_host}/{es_index}")
        return es_index

class ElasticsearchClient:
    def __init__(self, host: str):
        self.host = host
    
    def create_index(self, index_name: str) -> None:
        print(f"Creating Elasticsearch index: {index_name}")
    
    def index_exists(self, index_name: str) -> bool:
        # Simulate index existence check
        return False

class SlackClient:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    def send_message(self, message: str) -> None:
        print(f"Slack notification: {message}")

# Resource definitions
@resource(config_schema={"webhook_url": str})
def slack_resource(context):
    return SlackClient(context.resource_config["webhook_url"])

@resource(config_schema={"bucket": str})
def s3_resource(context):
    return S3Client()

@resource(config_schema={"host": str})
def elasticsearch_resource(context):
    return ElasticsearchClient(context.resource_config["host"])

@resource(config_schema={"jar_path": str, "obo_parser_jar": str})
def spark_resource(context):
    return SparkClient(
        context.resource_config["jar_path"],
        context.resource_config["obo_parser_jar"]
    )

# Failure hook for Slack notifications
@failure_hook(required_resource_keys={"slack"})
def slack_failure_hook(context: HookContext):
    message = f"❌ Mondo ETL pipeline failed at op: {context.op.name}"
    context.resources.slack.send_message(message)

# Pipeline ops
@op(
    config_schema={"color": str},
    out={"color": Out(str)},
    description="Validates the color parameter"
)
def params_validate(context) -> str:
    """Validates the color parameter provided to the pipeline."""
    color = context.op_config["color"]
    if not color or len(color.strip()) == 0:
        raise ValueError("Color parameter cannot be empty")
    context.log.info(f"Validated color: {color}")
    return color

@op(
    required_resource_keys={"s3"},
    config_schema={
        "github_repo": str,
        "s3_bucket": str,
        "s3_key_prefix": str,
    },
    out={"download_result": Out(Dict[str, Any])