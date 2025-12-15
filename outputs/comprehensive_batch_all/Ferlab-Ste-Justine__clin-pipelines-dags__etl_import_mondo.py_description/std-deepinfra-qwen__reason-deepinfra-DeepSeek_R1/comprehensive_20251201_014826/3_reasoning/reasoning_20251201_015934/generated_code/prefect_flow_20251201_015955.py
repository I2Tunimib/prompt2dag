<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x Python module. Let's start by understanding the requirements. The pipeline has several steps that must be executed sequentially. Each step should be a Prefect task, and the main flow will orchestrate them in order.

First, I'll import the necessary modules: prefect.flow and prefect.task. Also, since some tasks involve interacting with external services like S3, Spark, Elasticsearch, and Slack, I'll need to include relevant parameters and client initializations, even if they're placeholders.

The pipeline steps are params_validate, download_mondo_terms, normalized_mondo_terms, index_mondo_terms, publish_mondo, and slack. All are sequential, so each task will be called normally without .submit().

The parameters mentioned are color, spark_jar, and obo_parser_spark_jar. These should be parameters of the flow function. The flow will take these as arguments and pass them to the relevant tasks.

For each task, I'll define a @task-decorated function. The params_validate task needs to validate the color parameter. Maybe check if it's a valid color, but for simplicity, just a placeholder check.

The download_mondo_terms task will download the OBO file from GitHub, check if a newer version exists, and upload to S3. Since actual implementation details aren't provided, I'll add placeholders with comments.

The normalized_mondo_terms task uses Spark to process the data. I'll include a Spark session initialization, but in reality, this might be handled differently. Similarly, index_mondo_terms will use Spark to index into Elasticsearch.

The publish step is vague, so a placeholder function. The slack task sends a notification; I'll use the Prefect Slack integration, which requires a webhook URL. Here, I'll use a placeholder.

Failure handling uses NONE_FAILED trigger rule. Wait, Prefect 2 uses task runners and states. Maybe the flow should handle task failures, but the user mentioned using NONE_FAILED trigger rule, which in Prefect might relate to task retries or conditional execution. However, since all tasks are sequential, perhaps each task depends on the previous one's success. But the user mentioned that tasks can be skipped if data is up-to-date. So some tasks might have skip conditions. But the code needs to reflect that. However, without specific logic, I'll proceed with linear execution, assuming each task runs after the previous completes.

The __main__ guard will call the flow with sample parameters.

Now, structuring the code:

1. Import statements.
2. Define each task as a function with @task decorator.
3. The main flow function with @flow, taking parameters.
4. Each task called in sequence, passing necessary parameters.
5. Handle Slack notification on completion.

Wait, the user mentioned Slack notifications for task failures. But the pipeline step is to send a notification upon completion. So the slack task is the last step. For failure handling, maybe the flow should have error handling, but the user's description says the failure handling uses NONE_FAILED trigger rule and includes Slack notifications for failures. But Prefect 2 has mechanisms for notifications on failure, perhaps using blocks or hooks. However, the user's pipeline step includes a Slack task upon completion. So perhaps the slack task is only for success. For failure notifications, maybe it's out of scope here since the code only includes the steps listed.

So the code will have the slack task as the last step. If any task fails, the flow would stop, and the slack task wouldn't run. But the user's description says the failure handling includes Slack notifications for task failures. Hmm. Maybe the flow should have a finally block that sends a Slack message regardless of success or failure. But the pipeline step says "sends a Slack notification upon completion", which implies on success. The user might need to handle that separately, but given the code constraints, perhaps the slack task is the last step and only runs if all previous tasks succeed. However, the user's skip logic allows tasks to be skipped if data is up-to-date. So maybe some tasks can be skipped, but the slack task still runs. But in Prefect, skipped tasks don't block subsequent tasks if the trigger rule is set appropriately. But since all tasks are sequential, each task is called after the previous, so if a task is skipped (returns None?), the next task still runs. But the code as per the user's description should have strict sequential dependencies. So perhaps each task is called in order, and if a task decides to skip processing (like download_mondo_terms if no new version), the next tasks still execute but perhaps do nothing. Alternatively, the tasks might have conditional logic internally.

But the code provided should be minimal and use placeholders where necessary. So each task function will have a docstring explaining its purpose and some placeholder logic.

Putting it all together:

- The flow function takes parameters color, spark_jar, obo_parser_spark_jar.
- Each task is called in order, passing the necessary parameters.
- The Slack task is called at the end.

Now, code:

Import prefect.flow and prefect.task.

Define each task with appropriate parameters. For example, download_mondo_terms might need the color parameter. The normalized_mondo_terms task might need the spark_jar and obo_parser_spark_jar.

Wait, the parameters are configurable: color, spark_jar, obo_parser_spark_jar. So these are flow parameters passed to the tasks that need them.

So in the flow:

@flow
def import_mondo_ontology(
    color: str = "production",
    spark_jar: str = "s3://path/to/spark.jar",
    obo_parser_spark_jar: str = "s3://path/to/obo-parser.jar",
):
    # call tasks in order

Each task that needs these parameters will receive them as arguments.

For example, params_validate validates the color, so it takes color as an argument.

download_mondo_terms might use color to determine the S3 path.

But since the actual implementation isn't specified, the tasks will just receive the parameters and have placeholders.

Now, code:

from prefect import flow, task

@task
def params_validate(color: str):
    """Validate the color parameter."""
    # Example validation logic
    if color not in ["production", "staging"]:
        raise ValueError("Invalid color parameter")
    return True

@task
def download_mondo_terms(color: str):
    """Download Mondo OBO file from GitHub and upload to S3 if new version exists."""
    # Placeholder: Actual implementation would check versions and download
    print(f"Downloading Mondo terms for {color} environment")
    return "s3://bucket/mondo.obo"

@task
def normalized_mondo_terms(obo_file: str, obo_parser_spark_jar: str):
    """Process and normalize Mondo terms using Spark."""
    # Placeholder: Spark processing logic
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("NormalizeMondo").getOrCreate()
    # ... processing steps ...
    print("Normalized Mondo terms stored in data lake")
    return "s3://normalized/mondo"

@task
def index_mondo_terms(normalized_path: str, spark_jar: str):
    """Index normalized terms to Elasticsearch using Spark."""
    # Placeholder: Spark job with Elasticsearch connector
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.config("spark.jars", spark_jar).getOrCreate()
    # ... indexing steps ...
    print("Data indexed to Elasticsearch")
    return "elasticsearch-index-name"

@task
def publish_mondo(index_name: str):
    """Publish indexed Mondo data for consumption."""
    # Placeholder: Publishing steps
    print(f"Published Mondo data from index {index_name}")

@task
def slack():
    """Send Slack notification on pipeline completion."""
    # Placeholder: Use Prefect Slack integration
    print("Slack notification sent: Pipeline completed successfully")

@flow(name="import-mondo-ontology")
def import_mondo_ontology(
    color: str = "production",
    spark_jar: str = "s3://path/to/spark.jar",
    obo_parser_spark_jar: str = "s3://path/to/obo-parser.jar",
):
    # Orchestrate tasks in sequence
    params_validate(color)
    obo_file = download_mondo_terms(color)
    normalized_path = normalized_mondo_terms(obo_file, obo_parser_spark_jar)
    index_name = index_mondo_terms(normalized_path, spark_jar)
    publish_mondo(index_name)
    slack()

if __name__ == "__main__":
    import_mondo_ontology()

Wait, but in Prefect 2, tasks are called within the flow, and their dependencies are managed by the order of execution. So each task is called sequentially, and the output of one is passed to the next. That's correct.

But in the normalized_mondo_terms task, the obo_parser_spark_jar parameter is passed, which comes from the flow's parameter. Similarly, the spark_jar is passed to index_mondo_terms.

The params_validate task is called first, and it returns True, but that's not used. However, in Prefect, even if the output isn't used, the task is still part of the flow's dependency graph. So if params_validate raises an error, the flow stops.

The download_mondo_terms task takes color as a parameter, which comes from the flow's argument.

Each subsequent task uses the output of the previous task as input. For example, obo_file is the result of download_mondo_terms, which is passed to normalized_mondo_terms.

The normalized_mondo_terms task returns a path, which is passed to index_mondo_terms, and so on.

The Slack task is the last step and doesn't depend on any data, just the completion of the previous task.

This code should be PEP 8 compliant, uses Prefect 2.x style, and includes all the steps in order. The tasks are properly decorated, and the flow is called in the main guard.

I need to check if all parameters are correctly passed. The flow has parameters color, spark_jar, obo_parser_spark_jar. The normalized_mondo_terms task requires obo_parser_spark_jar, which is passed from the flow's parameter. The index_mondo_terms task uses spark_jar from the flow's parameter.

Yes, that's correct.

The tasks have docstrings explaining their purpose. Placeholder print statements are used where actual implementation would be.

The code doesn't include any branching or parallelism, as per the pipeline's linear execution.

Failure handling: If any task fails, the flow stops, and subsequent tasks aren't run. The Slack notification is only sent if all previous tasks complete successfully. However, the user mentioned that tasks can be skipped if data is up-to-date. For example, download_mondo_terms might skip processing if no new version is found. But in the current code, download_mondo_terms returns a string regardless. To handle skips, perhaps the task could return None, and subsequent tasks check for that. But the user's description says the pipeline follows a linear execution pattern with no branching, so perhaps all tasks are executed regardless, but internally they may skip processing. However, the code provided doesn't include such logic, as it's beyond the scope of the example.

The code meets all the constraints: uses Prefect 2.x tasks and flow, sequential execution, parameters passed correctly, and includes the main guard.
</think>

from prefect import flow, task

@task
def params_validate(color: str):
    """Validate color parameter for environment configuration"""
    if color not in ["production", "staging"]:
        raise ValueError(f"Invalid color parameter: {color}")
    return True

@task
def download_mondo_terms(color: str) -> str:
    """Download Mondo OBO file from GitHub and upload to S3 if newer version exists"""
    # Implementation would check GitHub releases and S3 versions
    s3_path = f"s3://mondo-bucket/{color}/mondo.obo"
    print(f"Downloaded latest Mondo terms to {s3_path}")
    return s3_path

@task
def normalized_mondo_terms(obo_path: str, obo_parser_spark_jar: str) -> str:
    """Process Mondo terms using Spark for normalization"""
    # Spark session initialization and processing logic
    print(f"Normalizing terms from {obo_path} using {obo_parser_spark_jar}")
    return f"s3://processed-data/{obo_path.split('/')[-1]}"

@task
def index_mondo_terms(processed_path: str, spark_jar: str) -> str:
    """Index normalized terms to Elasticsearch using Spark connector"""
    # Elasticsearch indexing configuration and execution
    print(f"Indexing {processed_path} with Spark jar {spark_jar}")
    return "mondo-index-2023"

@task
def publish_mondo(index_name: str):
    """Publish indexed data for consumption"""
    print(f"Publishing index {index_name} to public API")

@task
def slack():
    """Send pipeline completion notification to Slack"""
    # Would use Prefect Slack integration with webhook
    print("Success notification sent to #mondo-pipeline channel")

@flow(name="mondo-ontology-pipeline")
def import_mondo_ontology(
    color: str = "production",
    spark_jar: str = "s3://spark-jars/elasticsearch-connector.jar",
    obo_parser_spark_jar: str = "s3://spark-jars/obo-parser.jar"
):
    validation = params_validate(color)
    raw_path = download_mondo_terms(color)
    processed_path = normalized_mondo_terms(raw_path, obo_parser_spark_jar)
    index_name = index_mondo_terms(processed_path, spark_jar)
    publish_mondo(index_name)
    slack()

if __name__ == "__main__":
    import_mondo_ontology()