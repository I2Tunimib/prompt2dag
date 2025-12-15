from dagster import op, job, resource, RetryPolicy, Failure, Field, String, Int

# Simplified resource example
@resource(config_schema={"data_dir": Field(String, default_value="/app/data")})
def data_directory(init_context):
    return init_context.resource_config["data_dir"]

@resource(config_schema={"api_token": Field(String, is_required=True)})
def here_api_token(init_context):
    return init_context.resource_config["api_token"]

@op(required_resource_keys={"data_directory"})
def load_and_modify_data(context):
    """Ingest CSV files and convert them into JSON format."""
    data_dir = context.resources.data_directory
    # Simulate API call to load-and-modify service
    context.log.info(f"Loading and modifying data from {data_dir}")
    # Generate JSON files
    context.log.info("Generated table_data_{}.json files")

@op(required_resource_keys={"data_directory", "here_api_token"})
def data_reconciliation(context):
    """Standardize and reconcile city names using HERE geocoding service."""
    data_dir = context.resources.data_directory
    api_token = context.resources.here_api_token
    # Simulate API call to reconciliation service
    context.log.info(f"Reconciling data from {data_dir} using HERE API token: {api_token}")
    # Generate JSON files
    context.log.info("Generated reconciled_table_{}.json files")

@op(required_resource_keys={"data_directory"})
def openmeteo_data_extension(context):
    """Enrich the dataset with weather information."""
    data_dir = context.resources.data_directory
    # Simulate API call to OpenMeteo service
    context.log.info(f"Extending data with weather information from {data_dir}")
    # Generate JSON files
    context.log.info("Generated open_meteo_{}.json files")

@op(required_resource_keys={"data_directory"})
def column_extension(context):
    """Append additional data properties."""
    data_dir = context.resources.data_directory
    # Simulate API call to column extension service
    context.log.info(f"Extending columns in data from {data_dir}")
    # Generate JSON files
    context.log.info("Generated column_extended_{}.json files")

@op(required_resource_keys={"data_directory"})
def save_final_data(context):
    """Consolidate and export the fully enriched dataset."""
    data_dir = context.resources.data_directory
    # Simulate saving final CSV file
    context.log.info(f"Saving final data to {data_dir}")
    # Generate CSV file
    context.log.info("Generated enriched_data_{}.csv file")

@job(
    resource_defs={
        "data_directory": data_directory,
        "here_api_token": here_api_token,
    },
    retry_policy=RetryPolicy(max_retries=1)
)
def data_processing_pipeline():
    save_final_data(
        column_extension(
            openmeteo_data_extension(
                data_reconciliation(
                    load_and_modify_data()
                )
            )
        )
    )

if __name__ == '__main__':
    result = data_processing_pipeline.execute_in_process(
        run_config={
            "resources": {
                "data_directory": {"config": {"data_dir": "/app/data"}},
                "here_api_token": {"config": {"api_token": "your_here_api_token"}}
            }
        }
    )