from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule


def validate_params(**context):
    """
    Validate pipeline parameters.
    Expected parameters: color, spark_jar, obo_parser_spark_jar.
    """
    params = context["dag_run"].conf or {}
    required = ["color", "spark_jar", "obo_parser_spark_jar"]
    missing = [p for p in required if p not in params]
    if missing:
        raise ValueError(f"Missing required parameters: {', '.join(missing)}")
    # Example validation: ensure color is a non‑empty string
    if not isinstance(params["color"], str) or not params["color"]:
        raise ValueError("Parameter 'color' must be a non‑empty string")


def download_mondo_terms(**context):
    """
    Download the latest Mondo OBO file from GitHub releases.
    If a newer version exists compared to the one stored in S3,
    upload the new file to S3.
    """
    import json
    import os
    import tempfile
    import urllib.request

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # Configuration
    github_repo = "monarch-initiative/mondo"
    s3_bucket = Variable.get("mondo_raw_bucket", default_var="mondo-raw")
    s3_key_prefix = Variable.get("mondo_raw_key_prefix", default_var="raw/")
    s3 = S3Hook(aws_conn_id="aws_default")

    # Determine latest release via GitHub API
    api_url = f"https://api.github.com/repos/{github_repo}/releases/latest"
    with urllib.request.urlopen(api_url) as response:
        release_info = json.loads(response.read().decode())
    latest_tag = release_info.get("tag_name")
    if not latest_tag:
        raise RuntimeError("Unable to determine latest Mondo release tag")

    # Check if the version already exists in S3
    s3_key = f"{s3_key_prefix}mondo-{latest_tag}.obo"
    if s3.check_for_key(s3_key, bucket_name=s3_bucket):
        # File already present – skip download
        context["ti"].xcom_push(key="download_skipped", value=True)
        return

    # Find OBO asset URL
    obo_asset = next(
        (a for a in release_info["assets"] if a["name"].endswith(".obo")),
        None,
    )
    if not obo_asset:
        raise RuntimeError("No OBO asset found in the latest release")

    obo_url = obo_asset["browser_download_url"]
    # Download to a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        urllib.request.urlretrieve(obo_url, tmp_file.name)
        tmp_path = tmp_file.name

    # Upload to S3
    s3.load_file(
        filename=tmp_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True,
    )
    os.remove(tmp_path)
    context["ti"].xcom_push(key="download_skipped", value=False)


def normalize_mondo_terms(**context):
    """
    Process the downloaded OBO file using Spark to normalize ontology terms.
    Results are stored back to S3 (data lake).
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from pyspark.sql import SparkSession

    # Retrieve parameters
    params = context["dag_run"].conf or {}
    spark_jar = params.get("spark_jar")
    obo_parser_jar = params.get("obo_parser_spark_jar")
    s3_bucket = Variable.get("mondo_raw_bucket", default_var="mondo-raw")
    s3_key_prefix = Variable.get("mondo_raw_key_prefix", default_var="raw/")
    processed_prefix = Variable.get("mondo_processed_prefix", default_var="processed/")

    # Determine which OBO file to process
    s3 = S3Hook(aws_conn_id="aws_default")
    keys = s3.list_keys(bucket_name=s3_bucket, prefix=s3_key_prefix)
    if not keys:
        raise RuntimeError("No OBO files found in S3 raw bucket")
    latest_key = sorted(keys)[-1]  # simplistic latest selection
    local_obo_path = f"/tmp/{os.path.basename(latest_key)}"
    s3.download_file(key=latest_key, bucket_name=s3_bucket, local_path=local_obo_path)

    # Spark job (simplified placeholder)
    spark = (
        SparkSession.builder.appName("NormalizeMondo")
        .config("spark.jars", f"{spark_jar},{obo_parser_jar}")
        .getOrCreate()
    )
    # Placeholder for actual parsing logic
    df = spark.read.format("text").load(local_obo_path)
    normalized_df = df  # In real code, apply transformations

    output_path = f"s3a://{s3_bucket}/{processed_prefix}normalized-{os.path.basename(latest_key)}"
    normalized_df.write.mode("overwrite").parquet(output_path)
    spark.stop()


def index_mondo_terms(**context):
    """
    Index the normalized Mondo terms into Elasticsearch using Spark.
    """
    from pyspark.sql import SparkSession

    params = context["dag_run"].conf or {}
    spark_jar = params.get("spark_jar")
    es_host = Variable.get("elasticsearch_host", default_var="http://elasticsearch:9200")
    es_index = Variable.get("elasticsearch_mondo_index", default_var="mondo")

    # Path to normalized data in S3
    s3_bucket = Variable.get("mondo_raw_bucket", default_var="mondo-raw")
    processed_prefix = Variable.get("mondo_processed_prefix", default_var="processed/")
    normalized_path = f"s3a://{s3_bucket}/{processed_prefix}*"

    spark = (
        SparkSession.builder.appName("IndexMondo")
        .config("spark.jars", spark_jar)
        .config("es.nodes", es_host.split("//")[1].split(":")[0])
        .config("es.port", es_host.split(":")[-1])
        .config("es.resource", f"{es_index}/_doc")
        .getOrCreate()
    )
    df = spark.read.parquet(normalized_path)
    # Placeholder: write to Elasticsearch
    df.write.format("org.elasticsearch.spark.sql").mode("overwrite").save()
    spark.stop()


def publish_mondo(**context):
    """
    Publish the indexed Mondo data (e.g., refresh index alias or notify downstream systems).
    """
    # Placeholder implementation – in real pipelines this could involve
    # updating an alias, notifying a catalog service, etc.
    import logging

    logging.info("Publishing Mondo index – placeholder operation completed.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="mondo_etl_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["etl", "mondo", "ontology"],
) as dag:

    params_validate = PythonOperator(
        task_id="params_validate",
        python_callable=validate_params,
        provide_context=True,
    )

    download_mondo = PythonOperator(
        task_id="download_mondo_terms",
        python_callable=download_mondo_terms,
        provide_context=True,
    )

    normalize_mondo = PythonOperator(
        task_id="normalized_mondo_terms",
        python_callable=normalize_mondo_terms,
        provide_context=True,
    )

    index_mondo = PythonOperator(
        task_id="index_mondo_terms",
        python_callable=index_mondo_terms,
        provide_context=True,
    )

    publish = PythonOperator(
        task_id="publish_mondo",
        python_callable=publish_mondo,
        provide_context=True,
    )

    slack_success = SlackWebhookOperator(
        task_id="slack_success",
        http_conn_id="slack_webhook_default",
        webhook_token=Variable.get("slack_webhook_token"),
        message="✅ Mondo ETL pipeline completed successfully.",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    slack_failure = SlackWebhookOperator(
        task_id="slack_failure",
        http_conn_id="slack_webhook_default",
        webhook_token=Variable.get("slack_webhook_token"),
        message="❌ Mondo ETL pipeline failed.",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Define linear dependencies
    params_validate >> download_mondo >> normalize_mondo >> index_mondo >> publish >> slack_success

    # Failure notification should fire if any upstream task fails
    for task in [params_validate, download_mondo, normalize_mondo, index_mondo, publish]:
        task >> slack_failure