from dagster import op, job, get_dagster_logger, RetryPolicy

# Constants
DATA_DIR = "/path/to/data"
HERE_API_TOKEN = "your_here_api_token"
GEOAPIFY_API_KEY = "your_geoapify_api_key"
RISK_CALCULATION_PARAMS = "your_risk_calculation_params"

# Resources
# Example resource stubs (if needed)
# class MyResource:
#     def __init__(self, config):
#         self.config = config

# @resource(config_schema={"api_key": str})
# def my_resource(context):
#     return MyResource(context.resource_config["api_key"])


@op
def load_and_modify_data():
    import requests
    import pandas as pd
    import json

    logger = get_dagster_logger()

    # Load CSV
    stations_csv_path = f"{DATA_DIR}/stations.csv"
    df = pd.read_csv(stations_csv_path)

    # Parse installation_date
    df['installation_date'] = pd.to_datetime(df['installation_date'])

    # Standardize location names
    df['location'] = df['location'].str.strip().str.title()

    # Convert to JSON
    table_data_json_path = f"{DATA_DIR}/table_data_2.json"
    df.to_json(table_data_json_path, orient='records')

    # Call load-and-modify service
    response = requests.post(
        "http://localhost:3003/load-and-modify",
        json={
            "DATASET_ID": 2,
            "DATE_COLUMN": "installation_date",
            "TABLE_NAME_PREFIX": "JOT_"
        }
    )

    if response.status_code != 200:
        logger.error(f"Failed to load and modify data: {response.text}")
        raise Exception("Failed to load and modify data")

    return table_data_json_path


@op
def reconcile_geocoding(table_data_json_path):
    import requests
    import pandas as pd
    import json

    logger = get_dagster_logger()

    # Load JSON
    df = pd.read_json(table_data_json_path)

    # Call reconciliation service
    response = requests.post(
        "http://localhost:3003/reconciliation",
        json={
            "PRIMARY_COLUMN": "location",
            "RECONCILIATOR_ID": "geocodingHere",
            "API_TOKEN": HERE_API_TOKEN,
            "DATASET_ID": 2
        }
    )

    if response.status_code != 200:
        logger.error(f"Failed to reconcile geocoding: {response.text}")
        raise Exception("Failed to reconcile geocoding")

    # Parse response and add latitude/longitude
    geocoded_data = response.json()
    df['latitude'] = geocoded_data['latitude']
    df['longitude'] = geocoded_data['longitude']

    # Save to JSON
    reconciled_table_json_path = f"{DATA_DIR}/reconciled_table_2.json"
    df.to_json(reconciled_table_json_path, orient='records')

    return reconciled_table_json_path


@op
def extend_with_openmeteo_data(reconciled_table_json_path):
    import requests
    import pandas as pd
    import json

    logger = get_dagster_logger()

    # Load JSON
    df = pd.read_json(reconciled_table_json_path)

    # Call OpenMeteo service
    weather_data = []
    for _, row in df.iterrows():
        response = requests.get(
            f"https://api.open-meteo.com/v1/forecast?latitude={row['latitude']}&longitude={row['longitude']}&start_date={row['installation_date'].strftime('%Y-%m-%d')}&end_date={row['installation_date'].strftime('%Y-%m-%d')}&hourly=apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_hours"
        )

        if response.status_code != 200:
            logger.error(f"Failed to get weather data for {row['location']}: {response.text}")
            raise Exception("Failed to get weather data")

        weather_data.append(response.json())

    # Parse and add weather data
    df['apparent_temperature_max'] = [data['hourly']['apparent_temperature_max'][0] for data in weather_data]
    df['apparent_temperature_min'] = [data['hourly']['apparent_temperature_min'][0] for data in weather_data]
    df['precipitation_sum'] = [data['hourly']['precipitation_sum'][0] for data in weather_data]
    df['precipitation_hours'] = [data['hourly']['precipitation_hours'][0] for data in weather_data]

    # Save to JSON
    open_meteo_json_path = f"{DATA_DIR}/open_meteo_2.json"
    df.to_json(open_meteo_json_path, orient='records')

    return open_meteo_json_path


@op
def extend_with_land_use_data(open_meteo_json_path):
    import requests
    import pandas as pd
    import json

    logger = get_dagster_logger()

    # Load JSON
    df = pd.read_json(open_meteo_json_path)

    # Call GIS Land Use API
    land_use_data = []
    for _, row in df.iterrows():
        response = requests.get(
            f"https://api.geoapify.com/v2/places?lat={row['latitude']}&lon={row['longitude']}&categories=land_use&apiKey={GEOAPIFY_API_KEY}"
        )

        if response.status_code != 200:
            logger.error(f"Failed to get land use data for {row['location']}: {response.text}")
            raise Exception("Failed to get land use data")

        land_use_data.append(response.json())

    # Parse and add land use data
    df['land_use_type'] = [data['features'][0]['properties']['category'] if data['features'] else None for data in land_use_data]

    # Save to JSON
    land_use_json_path = f"{DATA_DIR}/land_use_2.json"
    df.to_json(land_use_json_path, orient='records')

    return land_use_json_path


@op
def extend_with_population_density_data(land_use_json_path):
    import requests
    import pandas as pd
    import json

    logger = get_dagster_logger()

    # Load JSON
    df = pd.read_json(land_use_json_path)

    # Call Demographic Data Service
    pop_density_data = []
    for _, row in df.iterrows():
        response = requests.get(
            f"https://api.worldpop.org/v1/data?lat={row['latitude']}&lon={row['longitude']}&radius=5000"
        )

        if response.status_code != 200:
            logger.error(f"Failed to get population density data for {row['location']}: {response.text}")
            raise Exception("Failed to get population density data")

        pop_density_data.append(response.json())

    # Parse and add population density data
    df['population_density'] = [data['population_density'] for data in pop_density_data]

    # Save to JSON
    pop_density_json_path = f"{DATA_DIR}/pop_density_2.json"
    df.to_json(pop_density_json_path, orient='records')

    return pop_density_json_path


@op
def extend_with_environmental_risk_data(pop_density_json_path):
    import pandas as pd
    import json

    logger = get_dagster_logger()

    # Load JSON
    df = pd.read_json(pop_density_json_path)

    # Compute risk score
    df['risk_score'] = df.apply(
        lambda row: (
            row['precipitation_sum'] * 0.3 +
            row['population_density'] * 0.5 +
            (1 if row['land_use_type'] == 'urban' else 0) * 0.2
        ),
        axis=1
    )

    # Save to JSON
    column_extended_json_path = f"{DATA_DIR}/column_extended_2.json"
    df.to_json(column_extended_json_path, orient='records')

    return column_extended_json_path


@op
def save_final_data(column_extended_json_path):
    import pandas as pd
    import json

    logger = get_dagster_logger()

    # Load JSON
    df = pd.read_json(column_extended_json_path)

    # Save to CSV
    final_csv_path = f"{DATA_DIR}/enriched_data_2.csv"
    df.to_csv(final_csv_path, index=False)

    return final_csv_path


@job(
    resource_defs={
        # 'my_resource': my_resource
    },
    retry_policy=RetryPolicy(max_retries=1)
)
def environmental_monitoring_pipeline():
    table_data_json_path = load_and_modify_data()
    reconciled_table_json_path = reconcile_geocoding(table_data_json_path)
    open_meteo_json_path = extend_with_openmeteo_data(reconciled_table_json_path)
    land_use_json_path = extend_with_land_use_data(open_meteo_json_path)
    pop_density_json_path = extend_with_population_density_data(land_use_json_path)
    column_extended_json_path = extend_with_environmental_risk_data(pop_density_json_path)
    save_final_data(column_extended_json_path)


if __name__ == '__main__':
    result = environmental_monitoring_pipeline.execute_in_process()