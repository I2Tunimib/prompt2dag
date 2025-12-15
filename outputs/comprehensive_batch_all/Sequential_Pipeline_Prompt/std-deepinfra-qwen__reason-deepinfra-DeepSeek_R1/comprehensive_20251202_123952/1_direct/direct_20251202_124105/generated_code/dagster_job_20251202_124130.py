from dagster import op, job, resource, RetryPolicy, Failure, Field, String, Int

# Simplified resource example for API token and data directory
@resource(config_schema={"api_token": Field(String, default_value="default_token"), "data_dir": Field(String, default_value="/app/data")})
def api_and_data_dir_resource(context):
    return {
        "api_token": context.resource_config["api_token"],
        "data_dir": context.resource_config["data_dir"]
    }

# Load and Modify Data
@op(required_resource_keys={"api_and_data_dir"})
def load_and_modify_data(context):
    """Ingest CSV files and convert them to JSON format."""
    data_dir = context.resources.api_and_data_dir["data_dir"]
    dataset_id = 2
    date_column = "Fecha_id"
    table_naming_convention = "JOT_{}"
    
    # Simulate the process
    context.log.info(f"Loading and modifying data from {data_dir} with dataset_id={dataset_id}, date_column={date_column}, table_naming_convention={table_naming_convention}")
    # Output: table_data_{}.json
    return f"{data_dir}/table_data_{dataset_id}.json"

# Data Reconciliation
@op(required_resource_keys={"api_and_data_dir"})
def data_reconciliation(context, input_file):
    """Standardize and reconcile city names using the HERE geocoding service."""
    api_token = context.resources.api_and_data_dir["api_token"]
    primary_column = "City"
    optional_columns = ["County", "Country"]
    reconciliator_id = "geocodingHere"
    
    # Simulate the process
    context.log.info(f"Reconciling data from {input_file} with primary_column={primary_column}, optional_columns={optional_columns}, reconciliator_id={reconciliator_id}")
    # Output: reconciled_table_{}.json
    return f"{input_file.replace('table_data', 'reconciled_table')}"

# OpenMeteo Data Extension
@op(required_resource_keys={"api_and_data_dir"})
def openmeteo_data_extension(context, input_file):
    """Enrich the dataset with weather information."""
    weather_attributes = ["apparent_temperature_max", "apparent_temperature_min", "precipitation_sum", "precipitation_hours"]
    date_format_separator = "-"
    
    # Simulate the process
    context.log.info(f"Extending data from {input_file} with weather_attributes={weather_attributes}, date_format_separator={date_format_separator}")
    # Output: open_meteo_{}.json
    return f"{input_file.replace('reconciled_table', 'open_meteo')}"

# Column Extension
@op(required_resource_keys={"api_and_data_dir"})
def column_extension(context, input_file):
    """Append additional data properties as defined by integration parameters."""
    extender_id = "reconciledColumnExt"
    
    # Simulate the process
    context.log.info(f"Extending columns in {input_file} with extender_id={extender_id}")
    # Output: column_extended_{}.json
    return f"{input_file.replace('open_meteo', 'column_extended')}"

# Save Final Data
@op(required_resource_keys={"api_and_data_dir"})
def save_final_data(context, input_file):
    """Consolidate and export the fully enriched dataset."""
    data_dir = context.resources.api_and_data_dir["data_dir"]
    
    # Simulate the process
    context.log.info(f"Saving final data from {input_file} to {data_dir}")
    # Output: enriched_data_{}.csv
    return f"{data_dir}/enriched_data.csv"

# Define the job
@job(resource_defs={"api_and_data_dir": api_and_data_dir_resource}, retry_policy=RetryPolicy(max_retries=1))
def data_processing_pipeline():
    input_file = load_and_modify_data()
    reconciled_file = data_reconciliation(input_file)
    openmeteo_file = openmeteo_data_extension(reconciled_file)
    extended_file = column_extension(openmeteo_file)
    save_final_data(extended_file)

# Launch pattern
if __name__ == '__main__':
    result = data_processing_pipeline.execute_in_process()