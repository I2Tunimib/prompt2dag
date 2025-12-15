from dagster import (
    op,
    job,
    resource,
    Field,
    String,
    Int,
    Array,
    OpExecutionContext,
    RetryPolicy,
    get_dagster_logger,
)
import json
import csv
import os


@resource(
    config_schema={
        "DATA_DIR": Field(String, default_value="/tmp/environmental_data"),
        "DATASET_ID": Field(Int, default_value=2),
        "DATE_COLUMN": Field(String, default_value="installation_date"),
        "TABLE_NAME_PREFIX": Field(String, default_value="JOT_"),
        "PRIMARY_COLUMN": Field(String, default_value="location"),
        "RECONCILIATOR_ID": Field(String, default_value="geocodingHere"),
        "API_TOKEN_HERE": Field(String, default_value="dummy_here_api_token"),
        "LAT_COLUMN": Field(String, default_value="latitude"),
        "LON_COLUMN": Field(String, default_value="longitude"),
        "WEATHER_VARIABLES": Field(
            Array(String),
            default_value=[
                "apparent_temperature_max",
                "apparent_temperature_min",
                "precipitation_sum",
                "precipitation_hours",
            ],
        ),
        "DATE_SEPARATOR_FORMAT": Field(String, default_value="YYYYMMDD"),
        "OUTPUT_COLUMN_LAND_USE": Field(String, default_value="land_use_type"),
        "API_KEY_GEOAPIFY": Field(String, default_value="dummy_geoapify_key"),
        "OUTPUT_COLUMN_POP_DENSITY": Field(String, default_value="population_density"),
        "RADIUS": Field(Int, default_value=5000),
        "EXTENDER_ID": Field(String, default_value="environmentalRiskCalculator"),
        "INPUT_COLUMNS": Field(
            Array(String),
            default_value=["precipitation_sum", "population_density", "land_use_type"],
        ),
        "OUTPUT_COLUMN_RISK": Field(String, default_value="risk_score"),
    }
)
def pipeline_config(init_context):
    """Resource providing configuration for the environmental monitoring pipeline."""
    return init_context.resource_config


@op(
    required_resource_keys={"pipeline_config"},
    retry_policy=RetryPolicy(max_retries=1),
    tags={"docker_image": "i2